/* UWAGA!
 * Poniższy program wysyła komunikat LOOKUP (szuka aktywnego nadajnika),
 * odbiera komunikat REPLY, podłącza się do grupy rozsyłania grupowego,
 * odbiera pakiety od nadajnika i wypisuje to, co dostaje na standardowe wyjście.
 *
 * Nie jest rozwiązaniem zadania z polecenia, tylko raczej potwierdzeniem, że komunikacja
 * z nadajnikiem zachodzi. Część oddana w komplecie to część A (nadajnik).
 */

#include <cstdio>
#include <iostream>
#include <string>
#include <cstdint>
#include <cstdlib>
#include </usr/include/boost/program_options.hpp>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <endian.h>
#include <thread>
#include <algorithm>
#include <mutex>
#include <vector>
#include <semaphore.h>

#include "structures.h"
#include "err.h"

#define MAX_UDP_DATAGRAM 65507

namespace po = boost::program_options;
std::string discover_addr;
std::string ctrl_port;
std::string ui_port;
int64_t BSIZE;
int64_t RTIME;
std::string NAME;
bool priority = false;

std::string current_address;
uint16_t current_port;
std::string current_name;

uint8_t *buffer = NULL;
int64_t PSIZE;
int64_t BSIZE_div_PSIZE;
uint64_t BYTE0;

std::string lookup = "ZERO_SEVEN_COME_IN\n";
std::string borewicz = "BOREWICZ_HERE";

size_t index_for_reader;
size_t max_fbn;
sem_t mutex_for_buffer;

bool reader_allowed_to_start = false;
sem_t starting_reader;

bool reader_asked_to_end = false;
sem_t reader_ending;

bool reader_waiting_for_data = false; // Dostęp do tej zmiennej jest koordynowany przez mutex for buffer
sem_t for_reader;

uint64_t session_id = 0;
std::vector<std::pair<uint64_t, uint16_t>> what_is_in_cells;

std::thread reader_thread;

namespace {
    uint16_t read_port(char *string, char **port_char) {
        errno = 0;
        unsigned long port = strtoul(string, NULL, 10);
        if (errno != 0) {
            delete[] (*port_char);
        }
        PRINT_ERRNO();
        if (port > UINT16_MAX) {
            delete[] (*port_char);
            fatal("%u is not a valid port number", port);
        }

        return (uint16_t) port;
    }

    size_t read_message(int socket_fd, struct sockaddr_in *client_address, void *message, size_t max_length, int flags,
                        char *src_char) {
        socklen_t
        address_length = (socklen_t)
        sizeof(*client_address);
        errno = 0;
        ssize_t len = recvfrom(socket_fd, message, max_length, flags,
                               (struct sockaddr *) client_address, &address_length);
        if (len < 0) {
            free(src_char);
            PRINT_ERRNO();
        }
        return (size_t) len;
    }

    void read_program_options(int &argc, char ***argv) {

        po::options_description desc("Allowed options");
        desc.add_options()
                ("help,h", "produce help message")
                ("discover_addr,d", po::value<std::string>(&discover_addr)->default_value("255.255.255.255"),
                 "set discover address (optional)")
                ("ctrl_port,C", po::value<std::string>(&ctrl_port)->default_value("37863"),
                 "set control port (optional)")
                ("ui_port,U", po::value<std::string>(&ui_port)->default_value("17863"),
                 "set data port (optional)")
                ("bsize,b", po::value<int64_t>(&BSIZE)->default_value(65536),
                 "set buffer size (optional)")
                ("rtime,R", po::value<int64_t>(&RTIME)->default_value(250),
                 "set rtime (optional)")
                ("name,n", po::value<std::string>(&NAME),
                 "set sender name (optional)");

        po::variables_map vm;
        try {
            po::store(po::parse_command_line(argc, *argv, desc), vm);
            po::notify(vm);
        }
        catch (const std::exception &e) {
            std::cerr << e.what() << "\n";
            std::cerr << desc << "\n";
            exit(1);
        }

        if (vm.count("help")) {
            std::cout << desc;
            exit(1);
        }
        if (vm.count("name")) {
            priority = true;
        }
        if (BSIZE <= 0) {
            std::cerr << "BSIZE cannot be zero or below!\n";
            exit(1);
        }
        if (RTIME <= 0) {
            std::cerr << "RTIME cannot be zero or below!\n";
            exit(1);
        }
        if (priority) {
            if (empty(NAME)) {
                std::cerr << "NAME cannot be empty!\n";
                exit(1);
            } else if (NAME[0] == ' ') {
                std::cerr << "NAME cannot begin with space!\n";
                exit(1);
            } else if (NAME[NAME.length() - 1] == ' ') {
                std::cerr << "NAME cannot end with space!\n";
                exit(1);
            } else if (NAME.length() > 64) {
                std::cerr << "NAME cannot be longer than 64 characters!\n";
                exit(1);
            } else {
                size_t i = 0;
                while (i != NAME.length()) {
                    if ((int) NAME[i] < 32) {
                        std::cerr << "NAME has to consist of signs with ascii codes between 32 and 127!";
                        exit(1);
                    }
                    i++;
                }
            }
        }
    }

    void check_port_string_content(char *port) {
        int ind = 0;
        while (port[ind] != '\0') {
            if (!((int) port[ind] >= (int) ('0') && (int) port[ind] <= (int) ('9'))) {
                std::cerr << "Port number must consist of digits!\n";
                delete[] port;
                exit(1);
            }
            ind++;
        }
    }

    uint16_t check_and_read_port(std::string port) {
        size_t port_length = port.length();
        char *port_char = new char[port_length + 1];
        strcpy(port_char, port.c_str());
        check_port_string_content(port_char);
        uint16_t port_result = read_port(port_char, &port_char);
        delete[] port_char;
        return port_result;
    }

    struct sockaddr_in get_discover_address(uint16_t port) {
        size_t discover_addr_length = discover_addr.length();
        char *discover_char = new char[discover_addr_length + 1];
        strcpy(discover_char, discover_addr.c_str());

        struct sockaddr_in remote_address;
        remote_address.sin_family = AF_INET;
        remote_address.sin_port = htons(port);
        if (inet_aton(discover_char, &remote_address.sin_addr) == 0) {
            delete[] discover_char;
            std::cerr << "ERROR: inet_aton - invalid destination address\n";
            exit(EXIT_FAILURE);
        }

        delete[] discover_char;
        return remote_address;
    }

    ssize_t send_message(int socket_fd, const struct sockaddr_in *send_address,
                         const void *message, size_t length) {
        int send_flags = 0;
        socklen_t
        address_length = (socklen_t)
        sizeof(*send_address);
        errno = 0;
        ssize_t sent_length = sendto(socket_fd, message, length, send_flags,
                                     (struct sockaddr *) send_address, address_length);

        return sent_length;
    }

    void reader_thread_function() {
        sem_wait(&starting_reader);

        while (true) {
            sem_wait(&reader_ending);
            if (reader_asked_to_end) {
                reader_asked_to_end = false;
                sem_post(&reader_ending);
                return;
            } else {
                sem_post(&reader_ending);
            }
            sem_wait(&mutex_for_buffer);
            if (max_fbn >= (size_t) BSIZE_div_PSIZE * PSIZE) {
                index_for_reader = std::max(index_for_reader, max_fbn - BSIZE_div_PSIZE * PSIZE);
            }
            if (index_for_reader > max_fbn) {

                sem_wait(&reader_ending);
                if (reader_asked_to_end) {
                    reader_asked_to_end = false;
                    sem_post(&reader_ending);
                    sem_post(&mutex_for_buffer);
                    return;
                } else {
                    sem_post(&reader_ending);
                }

                sem_post(&mutex_for_buffer);
                reader_waiting_for_data = true;
                sem_wait(&for_reader);

                continue;
            }

            size_t cell_no = (index_for_reader / PSIZE) % BSIZE_div_PSIZE;
            size_t byte_no = cell_no * PSIZE;
            fwrite(buffer + byte_no, 1, PSIZE, stdout);
            memset(buffer + byte_no, 0, PSIZE);

            index_for_reader += PSIZE;
            sem_post(&mutex_for_buffer);
        }
    }

    void wait_for_reader_thread() {

        sem_wait(&reader_ending);
        reader_asked_to_end = true;
        sem_post(&reader_ending);

        if (!reader_allowed_to_start) {
            sem_post(&starting_reader);
        }

        sem_post(&mutex_for_buffer);
        if (index_for_reader > max_fbn) {
            sem_post(&for_reader);
        }
        sem_post(&mutex_for_buffer);

        reader_thread.join();
    }

    size_t next_cell(size_t cell) {
        if (cell > 0) {
            return cell - 1;
        } else {
            return BSIZE_div_PSIZE - 1;
        }
    }

    void set_port_reuse(int socket_fd) {
        int option_value = 1;
        CHECK_ERRNO(setsockopt(socket_fd, SOL_SOCKET, SO_REUSEPORT, &option_value, sizeof(option_value)));
    }

    void bind_socket(int socket_fd, uint16_t port) {
        struct sockaddr_in address;
        address.sin_family = AF_INET; // IPv4
        address.sin_addr.s_addr = htonl(INADDR_ANY); // listening on all interfaces
        address.sin_port = htons(port);

        set_port_reuse(socket_fd);

        // bind the socket to a concrete address
        CHECK_ERRNO(bind(socket_fd, (struct sockaddr *) &address,
                         (socklen_t) sizeof(address)));
    }

    int open_udp_socket() {
        int socket_fd = socket(PF_INET, SOCK_DGRAM, 0);
        if (socket_fd < 0) {
            PRINT_ERRNO();
        }
        set_port_reuse(socket_fd);
        return socket_fd;
    }

    void bind_socket_to_any_port(int socket_fd) {
        bind_socket(socket_fd, 0);
        return;
    }

    std::pair<int, int> number_from_address(char *address, int index) {
        int result_second = 1;
        int result_first = (int) address[index] - (int) ('0');
        while (address[index + result_second] != '.' && address[index + result_second] != '\0') {
            result_first *= 10;
            result_first += ((int) address[index + result_second] - (int) ('0'));
            result_second++;
        }
        return std::make_pair(result_first, result_second);
    }

    bool is_multicast(char *address) {
        std::pair<int, int> numbers[4];
        int index = 0;
        numbers[0] = number_from_address(address, index);
        index += (numbers[0].second + 1);
        for (int i = 1; i < 4; ++i) {
            numbers[i] = number_from_address(address, index);
            index += (numbers[i].second + 1);
        }
        if (numbers[0].first < 224 || numbers[0].first > 239) {
            return false;
        }
        /* According to lab scenario, multicast addresses are 224.0.0.1 - 239.255.255.255,
         * but according to other sources they are 224.0.0.0 - 239.255.255.255
        if (numbers[0].first == 224 && numbers[1].first == 0 && numbers[2].first == 0 && numbers[3].first == 0) {
            return false;
        }
        */
        return true;
    }

    bool checked_that_reply(char *buff, ssize_t length) {
        std::string block = "";
        std::string addr = "";
        std::string port_string = "";
        std::string name = "";
        ssize_t i = 0;
        while (i < length && buff[i] != ' ') {
            block += buff[i];
            i++;
        }
        for (size_t j = 0; j < block.size(); ++j) {
            if (block[j] != borewicz[j]) {
                return false;
            }
        }
        i++;
        while (i < length && buff[i] != ' ') {
            addr += buff[i];
            i++;
        }
        size_t mcast_addr_length = addr.length();
        char *mcast_char = new char[mcast_addr_length + 1];
        strcpy(mcast_char, addr.c_str());
        if (inet_aton(mcast_char, NULL) == 0 || !is_multicast(mcast_char)) {
            delete[] mcast_char;
            return false;
        }
        delete[] mcast_char;
        i++;
        while (i < length && buff[i] != ' ') {
            port_string += buff[i];
            i++;
        }
        size_t port_length = port_string.length();
        char *port_char = new char[port_length + 1];
        strcpy(port_char, port_string.c_str());
        unsigned long port = strtoul(port_char, NULL, 10);
        if (errno != 0 || port > UINT16_MAX) {
            delete[] port_char;
            return false;
        }
        delete[] port_char;
        i++;
        size_t counter = 0;
        while (i < length && counter < 64) {
            name += buff[i];
            i++;
            counter++;
        }
        if (i < length || name.length() == 0 || name[0] == ' ' || name[name.length() - 1] == ' ') {
            return false;
        }
        if (name[name.length() - 1] == '\n') {
            name.pop_back();
        }
        for (size_t k = 0; k < name.length(); ++k) {
            if ((int) name[k] < 32) {
                return false;
            }
        }
        current_address = addr;
        current_port = (uint16_t) port;
        current_name = name;

        return true;
    }

    int look_for_sender(int socket, struct sockaddr_in *discover_address) {
        ssize_t sent_length;
        size_t read_length;
        char *lookup_message = new char[lookup.size()];
        for (size_t i = 0; i < lookup.size(); ++i) {
            lookup_message[i] = lookup[i];
        }
        sent_length = send_message(socket, discover_address, lookup_message, lookup.size());
        if (sent_length < 0) {
            PRINT_ERRNO();
        }
        ENSURE(sent_length == (ssize_t) lookup.size());
        delete[] lookup_message;

        char *reply = new char[MAX_UDP_DATAGRAM];
        bool found_reply = false;
        while (!found_reply) {
            read_length = read_message(socket, NULL, reply, MAX_UDP_DATAGRAM, 0, NULL);
            if (checked_that_reply(reply, read_length)) {
                found_reply = true;
            }
        }
        delete[] reply;

        size_t mcast_addr_length = current_address.length();
        char *mcast_char = new char[mcast_addr_length + 1];
        strcpy(mcast_char, current_address.c_str());

        int new_socket = open_udp_socket();

        /* joining multicast group */
        struct ip_mreq ip_mreq;
        ip_mreq.imr_interface.s_addr = htonl(INADDR_ANY);
        if (inet_aton(mcast_char, &ip_mreq.imr_multiaddr) == 0) {
            fatal("inet_aton - invalid multicast address\n");
        }
        delete[] mcast_char;

        CHECK_ERRNO(setsockopt(new_socket, IPPROTO_IP, IP_ADD_MEMBERSHIP, (void *) &ip_mreq, sizeof ip_mreq));
        /* setting local address and port */
        bind_socket(new_socket, current_port);
        return new_socket;
    }
}
int main(int argc, char *argv[]) {

    read_program_options(argc, &argv);
    uint16_t port_ctrl = check_and_read_port(ctrl_port);
    //uint16_t port_ui = check_and_read_port(ui_port);
    struct sockaddr_in discover_address = get_discover_address(port_ctrl);

    int socket_fd = open_udp_socket();
    bind_socket_to_any_port(socket_fd);

    int optval = 1;
    CHECK_ERRNO(setsockopt(socket_fd, SOL_SOCKET, SO_BROADCAST, (void *) &optval, sizeof optval));

    struct sockaddr_in client_address;
    size_t read_length;

    int new_socket = look_for_sender(socket_fd, &discover_address);

    struct DataStructure data;
    try {
        buffer = new uint8_t[BSIZE];
    }
    catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        exit(1);
    }

    sem_init(&mutex_for_buffer, 0, 1);
    sem_init(&starting_reader, 0, 0);
    sem_init(&reader_ending, 0, 1);
    sem_init(&for_reader, 0, 0);

    for(;;) {
        read_length = read_message(new_socket, &client_address, &data, sizeof(data), 0, NULL);
        //char* client_ip = inet_ntoa(client_address.sin_addr);
        size_t new_session_id = be64toh(data.session_id);
        if (new_session_id >= session_id) {
            if (new_session_id > session_id) {
                if (session_id > 0) {
                    wait_for_reader_thread();
                }
                session_id = new_session_id;
                PSIZE = read_length - 16;
                if (PSIZE > BSIZE) {
                    std::cerr << "PSIZE cannot be greater than BSIZE!\n";
                    continue;
                }
                what_is_in_cells.clear();
                memset(buffer, 0, BSIZE);
                reader_allowed_to_start = false;
                BYTE0 = be64toh(data.first_byte_num);
                index_for_reader = BYTE0;
                max_fbn = BYTE0;

                BSIZE_div_PSIZE = BSIZE / PSIZE;
                for (int64_t i = 0; i < BSIZE_div_PSIZE; ++i) {
                    what_is_in_cells.push_back(std::make_pair(0, 0));
                }
                std::thread t{reader_thread_function};
                reader_thread = std::move(t);

                size_t cell_no = (max_fbn / PSIZE) % BSIZE_div_PSIZE;
                what_is_in_cells[cell_no].first = max_fbn;
                what_is_in_cells[cell_no].second = 1;

                size_t byte_no = cell_no * PSIZE;
                memcpy(buffer + byte_no, data.audio_data, PSIZE);

                if (max_fbn >= BYTE0 + BSIZE * 3 / 4) {
                    reader_allowed_to_start = true;
                    sem_post(&starting_reader);
                }
            }
            else {
                if (PSIZE > BSIZE) {
                    std::cerr << "PSIZE cannot be greater than BSIZE!\n";
                    continue;
                }

                sem_wait(&mutex_for_buffer);
                uint64_t new_fbn = be64toh(data.first_byte_num);
                if (new_fbn > BYTE0) {
                    // Checking if package is too old
                    // If max_fbn < BSIZE_div_PSIZE * PSIZE there is no need to check that
                    // because in this case package is not too old.
                    if (max_fbn >= (size_t)BSIZE_div_PSIZE * PSIZE) {
                        if (new_fbn <= max_fbn - BSIZE_div_PSIZE * PSIZE) {
                            sem_post(&mutex_for_buffer);
                            continue;
                        }
                    }
                    // Package is not too old
                    size_t cell_no = (new_fbn / PSIZE) % BSIZE_div_PSIZE;
                    what_is_in_cells[cell_no].first = new_fbn;
                    what_is_in_cells[cell_no].second = 1;

                    size_t byte_no = cell_no * PSIZE;
                    memcpy(buffer + byte_no, data.audio_data, PSIZE);

                    if (new_fbn > max_fbn) {
                        max_fbn = new_fbn;
                    }
                    // My index is cell_no
                    // Go through the packages (by subtractin from an index). Wait for index to be
                    // index of package with max first byte num

                    size_t temp_byte_no = new_fbn;
                    size_t max_fbn_cell_no = (max_fbn / PSIZE) % BSIZE_div_PSIZE;
                    size_t next = next_cell(cell_no);
                    while (next != max_fbn_cell_no) {
                        if (temp_byte_no < (size_t)PSIZE) {
                            break;
                        }
                        else {
                            temp_byte_no -= PSIZE;
                            if (what_is_in_cells[next].first != temp_byte_no) {
                                what_is_in_cells[next].first = temp_byte_no;
                                what_is_in_cells[next].second = 2;
                                //memset(buffer + temp_byte_no, 0, PSIZE);
                            }
                        }
                        next = next_cell(next);
                    }

                    if (next == max_fbn_cell_no) {
                        next = (next + 1) % BSIZE_div_PSIZE;
                    }
                    while (next != cell_no) {
                        if (what_is_in_cells[next].second == 2) {
                            // We don't write about missing, but the logic is here as preparation for rexmit requests.
                        }
                        next = (next + 1) % BSIZE_div_PSIZE;
                    }

                    if (max_fbn >= BYTE0 + BSIZE * 3 / 4) {
                        if (!reader_allowed_to_start) {
                            reader_allowed_to_start = true;
                            sem_post(&starting_reader);
                        }
                    }

                }
                if (reader_waiting_for_data) {
                    reader_waiting_for_data = false;
                    sem_post(&for_reader);
                }
                sem_post(&mutex_for_buffer);
            }
        }
    }

    sem_destroy(&mutex_for_buffer);
    sem_destroy(&starting_reader);
    sem_destroy(&reader_ending);
    sem_destroy(&for_reader);
    delete[] buffer;

    CHECK_ERRNO(close(socket_fd));
    CHECK_ERRNO(close(new_socket));

    exit(0);
}
