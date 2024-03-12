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
#include <vector>
#include <semaphore.h>
#include <poll.h>
#include <thread>
#include <chrono>
#include <unordered_set>

#include "structures.h"
#include "err.h"

#define MAX_UDP_DATAGRAM 65507

namespace po = boost::program_options;
std::string mcast_addr;
std::string data_port;
std::string ctrl_port;
int64_t PSIZE;
int64_t FSIZE;
int64_t RTIME;
std::string NAME;

uint64_t session_id = 0;
uint64_t current_package = 0;

sem_t for_FIFO;
int64_t FSIZE_div_PSIZE;
uint8_t* data_portion = NULL;
std::vector<uint8_t *> FIFO;
uint64_t min_fbn_in_FIFO = 0;
uint64_t max_fbn_in_FIFO = 0;

sem_t for_max_in_general;
uint64_t max_fbn_in_general = 0;
bool sent_at_least_first_package = false;

sem_t for_set_of_rexmits;
std::unordered_set<uint64_t> set_of_rexmits;

std::string lookup = "ZERO_SEVEN_COME_IN\n";
std::string rexmit = "LOUDER_PLEASE ";
std::string reply;
char *reply_buffer;

sem_t sem_sending_data_ended;
bool sending_data_ended = false;

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

    void read_program_options(int &argc, char ***argv) {

        po::options_description desc("Allowed options");
        desc.add_options()
                ("help,h", "produce help message")
                ("mcast_addr,a", po::value<std::string>(&mcast_addr),
                 "set multicast destination address (obligatory)")
                ("data_port,P", po::value<std::string>(&data_port)->default_value("27863"),
                 "set data port (optional)")
                ("ctrl_port,C", po::value<std::string>(&ctrl_port)->default_value("37863"),
                 "set control port (optional)")
                ("psize,p", po::value<int64_t>(&PSIZE)->default_value(512),
                 "set package size (optional)")
                ("fsize,f", po::value<int64_t>(&FSIZE)->default_value(128000),
                 "set fifo size (optional)")
                ("rtime,R", po::value<int64_t>(&RTIME)->default_value(250),
                 "set rtime (optional)")
                ("name,n", po::value<std::string>(&NAME)->default_value("Nienazwany nadajnik"),
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

        if (argc < 2) {
            std::cerr << "Usage: " << (*argv)[0] << " [options]\n";
            std::cerr << desc;
            exit(1);
        }

        if (vm.count("help")) {
            std::cout << desc;
            exit(1);
        }
        if (!vm.count("mcast_addr")) {
            std::cerr << "Setting multicast destination address is obligatory!\n";
            exit(1);
        }
        if (PSIZE <= 0) {
            std::cerr << "PSIZE cannot be zero or below!\n";
            exit(1);
        }
        if (PSIZE > MAX_UDP_DATAGRAM - 16) {
            std::cerr << "PSIZE cannot be greater than 65507 (max UDP datagram) - 16 (first two fields)!\n";
            exit(1);
        }
        if (FSIZE < 0) {
            std::cerr << "FSIZE cannot be below zero!\n";
            exit(1);
        }
        if (RTIME <= 0) {
            std::cerr << "RTIME cannot be zero or below!\n";
            exit(1);
        }
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

    struct sockaddr_in get_multicast_address(uint16_t port) {
        size_t mcast_addr_length = mcast_addr.length();
        char *mcast_char = new char[mcast_addr_length + 1];
        strcpy(mcast_char, mcast_addr.c_str());

        struct sockaddr_in remote_address;
        remote_address.sin_family = AF_INET;
        remote_address.sin_port = htons(port);
        if (inet_aton(mcast_char, &remote_address.sin_addr) == 0) {
            delete[] mcast_char;
            std::cerr << "ERROR: inet_aton - invalid destination address\n";
            exit(EXIT_FAILURE);
        } else {
            if (!is_multicast(mcast_char)) {
                delete[] mcast_char;
                std::cerr << "ERROR: destination address must be multicast\n";
                exit(EXIT_FAILURE);
            }
        }

        delete[] mcast_char;
        return remote_address;
    }

    void set_port_reuse(int socket_fd) {
        int option_value = 1;
        CHECK_ERRNO(setsockopt(socket_fd, SOL_SOCKET, SO_REUSEPORT, &option_value, sizeof(option_value)));
    }

    int bind_socket(uint16_t port) {
        int socket_fd = socket(AF_INET, SOCK_DGRAM, 0); // creating IPv4 UDP socket
        ENSURE(socket_fd >= 0);
        // after socket() call; we should close(sock) on any execution path;

        struct sockaddr_in server_address;
        server_address.sin_family = AF_INET; // IPv4
        server_address.sin_addr.s_addr = htonl(INADDR_ANY); // listening on all interfaces
        server_address.sin_port = htons(port);

        set_port_reuse(socket_fd);

        // bind the socket to a concrete address
        CHECK_ERRNO(bind(socket_fd, (struct sockaddr *) &server_address,
                         (socklen_t) sizeof(server_address)));

        return socket_fd;
    }

    int open_udp_socket() {
        int socket_fd = socket(PF_INET, SOCK_DGRAM, 0);
        if (socket_fd < 0) {
            PRINT_ERRNO();
        }
        set_port_reuse(socket_fd);
        return socket_fd;
    }

    size_t read_message(int socket_fd, struct sockaddr_in *client_address, char *buffer, size_t max_length) {
        socklen_t
        address_length = (socklen_t)
        sizeof(*client_address);
        int flags = 0; // we do not request anything special
        errno = 0;
        ssize_t len = recvfrom(socket_fd, buffer, max_length, flags,
                               (struct sockaddr *) client_address, &address_length);
        if (len < 0) {
            PRINT_ERRNO();
        }
        return (size_t) len;
    }

    void create_reply_content() {
        reply = "BOREWICZ_HERE " + mcast_addr + " " + data_port + " " + NAME + "\n";
        reply_buffer = new char[reply.size()];
        for (size_t i = 0; i < reply.size(); ++i) {
            reply_buffer[i] = reply[i];
        }
    }

    bool is_digit(char c) {
        if ((int) c >= (int) ('0') && (int) c <= (int) ('9')) {
            return true;
        }
        return false;
    }

    // only digits and commas, maybe ended with endline, no two commas next to each other, no comma at the beginning
    bool check_rexmit_format(char *buffer, size_t length, size_t position) {
        if (buffer[length - 1] == '\n') {
            length--;
        }
        if (length > position && buffer[position] == ',') {
            return false;
        }
        for (size_t i = position; i < length; ++i) {
            if (buffer[i] != ',' && !is_digit(buffer[i])) {
                return false;
            }
            if (buffer[i] == ',' && buffer[i - 1] == ',') { // i > 0 for sure
                return false;
            }
        }
        return true;
    }


    void check_numbers_and_store_if_ok(char *buffer, size_t length, size_t position, uint64_t max_value) {
        std::unordered_set <uint64_t> fbn_to_rexmit;
        uint64_t number;
        char last_character;
        if (position == length) {
            return;
        }
        // buffer content doesn't start with comma and doesn't have two commas one after another
        // if there is endline, it's at the end
        if (buffer[position] == '\n') {
            return;
        }
        if (buffer[length - 1] == '\n') {
            length--; // if there is ",\n" at the end of buffer, without that subtraction algorithm would put 0 into the set
        }
        // buffer[position] is a digit
        number = (int) buffer[position] - (int) ('0');
        last_character = buffer[position];
        position++;

        while (position < length) {
            if (is_digit(buffer[position])) {
                number *= 10;
                number += ((int) buffer[position] - (int) ('0'));
            } else {
                if (!(number % PSIZE == 0) || number > max_value) {
                    fbn_to_rexmit.clear();
                    return;
                }
                fbn_to_rexmit.insert(number);
                number = 0;
            }
            last_character = buffer[position];
            position++;
        }
        if (is_digit(last_character)) {
            if (!(number % PSIZE == 0) || number > max_value) {
                fbn_to_rexmit.clear();
                return;
            }
            fbn_to_rexmit.insert(number);
        }

        sem_wait(&for_set_of_rexmits);
        for (uint64_t num: fbn_to_rexmit) {
            set_of_rexmits.insert(num);
        }
        fbn_to_rexmit.clear();

        sem_post(&for_set_of_rexmits);
        return;
    }

    void check_if_rexmit_and_store_data(char *buffer, size_t received_bytes, uint64_t max_value) {
        bool begins_like_rexmit = true;
        for (size_t i = 0; i < rexmit.size(); ++i) {
            if (rexmit[i] != buffer[i]) {
                begins_like_rexmit = false;
                break;
            }
        }
        if (begins_like_rexmit) {
            bool digits_and_commas_only = check_rexmit_format(buffer, received_bytes, rexmit.size());
            if (digits_and_commas_only) {
                check_numbers_and_store_if_ok(buffer, received_bytes, rexmit.size(), max_value);
            }
        }
        return;
    }

    bool check_if_lookup_and_reply(int socket, struct sockaddr_in *client_address, char *buffer) {
        bool the_same = true;
        for (size_t i = 0; i < lookup.size(); ++i) {
            if (lookup[i] != buffer[i]) {
                the_same = false;
                break;
            }
        }
        if (the_same) {
            send_message(socket, client_address, reply_buffer, reply.size());
            return true;
        }
        return the_same;
    }

    void analyse_datagram_and_reply(int socket, struct sockaddr_in *client_address, char *buffer, size_t received_bytes) {
        bool it_was_lookup = false;
        if (received_bytes < rexmit.size()) {
            return;
        } else if (received_bytes == lookup.size()) {
            it_was_lookup = check_if_lookup_and_reply(socket, client_address, buffer);
        }
        if (!it_was_lookup) {
            sem_wait(&for_max_in_general);
            if (!sent_at_least_first_package) {
                sem_post(&for_max_in_general);
                return;
            }
            uint64_t max_value = max_fbn_in_general;
            sem_post(&for_max_in_general);
            check_if_rexmit_and_store_data(buffer, received_bytes, max_value);
        }
    }

    void rexmiting_thread(int socket, uint16_t port_to_send_to) {
        std::unordered_set <uint64_t> what_to_rexmit_now;
        struct DataStructure data_to_send;
        data_to_send.session_id = htobe64(session_id);
        struct sockaddr_in multicast_address = get_multicast_address(port_to_send_to);

        while (1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(RTIME));

            sem_wait(&for_set_of_rexmits);
            for (uint64_t num: set_of_rexmits) {
                what_to_rexmit_now.insert(num);
            }
            set_of_rexmits.clear();
            sem_post(&for_set_of_rexmits);

            if (FSIZE_div_PSIZE > 0) {
                sem_wait(&for_FIFO);
                for (uint64_t num: what_to_rexmit_now) {
                    if (num >= min_fbn_in_FIFO && num <= max_fbn_in_FIFO) {
                        int64_t position = (num - min_fbn_in_FIFO) / PSIZE;
                        data_to_send.first_byte_num = htobe64(num);
                        memcpy(data_to_send.audio_data, FIFO[position], PSIZE);
                        send_message(socket, &multicast_address, &data_to_send, (size_t) PSIZE + 16);
                    }
                }
                sem_post(&for_FIFO);
            }

            what_to_rexmit_now.clear();

            sem_wait(&sem_sending_data_ended);
            if (sending_data_ended) {
                sem_post(&sem_sending_data_ended);
                return;
            }
            sem_post(&sem_sending_data_ended);
        }
    }

    void LOOKUP_and_REXMIT_thread(int socket) {
        char buffer[MAX_UDP_DATAGRAM];
        int poll_status;
        struct sockaddr_in client_address;
        struct pollfd poll_descriptors[1];
        poll_descriptors[0].fd = socket;
        poll_descriptors[0].events = POLLIN;
        poll_descriptors[0].revents = 0;

        while (1) {
            poll_status = poll(poll_descriptors, 1, RTIME);
            if (poll_status > 0) {
                if (poll_descriptors[0].revents & POLLIN) {
                    size_t received_bytes = read_message(poll_descriptors[0].fd, &client_address, buffer,
                                                         MAX_UDP_DATAGRAM);
                    if (received_bytes > 0) {
                        analyse_datagram_and_reply(socket, &client_address, buffer, received_bytes);
                    }
                    poll_descriptors[0].revents = 0;
                }
            }
            sem_wait(&sem_sending_data_ended);
            if (sending_data_ended) {
                sem_post(&sem_sending_data_ended);
                return;
            }
            sem_post(&sem_sending_data_ended);
        }
    }
}
int main(int argc, char *argv[]) {

    session_id = time(NULL);

    read_program_options(argc, &argv);
    uint16_t port_data = check_and_read_port(data_port);
    uint16_t port_ctrl = check_and_read_port(ctrl_port);

    struct sockaddr_in multicast_address = get_multicast_address(port_data);

    create_reply_content();

    int server_socket = bind_socket(port_ctrl);
    int socket_fd = open_udp_socket();
    int rexmiting_socket = open_udp_socket();

    sem_init(&for_set_of_rexmits, 0, 1);
    sem_init(&for_max_in_general, 0, 1);
    sem_init(&for_FIFO, 0, 1);
    FSIZE_div_PSIZE = FSIZE / PSIZE;
    data_portion = new uint8_t[PSIZE + 1];

    sem_init(&sem_sending_data_ended, 0, 1);
    std::thread receiving_requests_thread{LOOKUP_and_REXMIT_thread, server_socket};
    std::thread replying_to_rexmit_requests_thread{rexmiting_thread, rexmiting_socket, port_data};

    struct DataStructure data_to_send;
    data_to_send.session_id = htobe64(session_id);
    ssize_t sent_length;

    size_t SIZE_T_PSIZE = (size_t)PSIZE;
    size_t result = fread(data_to_send.audio_data, sizeof(*(data_to_send.audio_data)), SIZE_T_PSIZE, stdin);

    while (result == SIZE_T_PSIZE) {
        uint64_t fbn = SIZE_T_PSIZE * current_package;

        data_to_send.first_byte_num = htobe64(fbn);
        current_package++;

        sem_wait(&for_FIFO);
        if (FSIZE_div_PSIZE > 0) {
            if (FIFO.size() == (uint64_t)FSIZE_div_PSIZE) {
                min_fbn_in_FIFO += PSIZE;
                delete[] FIFO[0];
                FIFO.erase(FIFO.begin());
            }
            max_fbn_in_FIFO = fbn;
            data_portion = new uint8_t[PSIZE + 1];
            memcpy(data_portion, data_to_send.audio_data, PSIZE);
            data_portion[PSIZE] = '\0';
            FIFO.push_back(data_portion);
        }
        sem_post(&for_FIFO);

        sem_wait(&for_max_in_general);
        sent_length = send_message(socket_fd, &multicast_address, &data_to_send, SIZE_T_PSIZE + 16);
        if (sent_length < 0) {
            PRINT_ERRNO();
        }
        ENSURE(sent_length == (ssize_t) (SIZE_T_PSIZE + 16));

        sent_at_least_first_package = true;
        max_fbn_in_general = fbn;
        sem_post(&for_max_in_general);

        result = fread(data_to_send.audio_data, sizeof(*(data_to_send.audio_data)), SIZE_T_PSIZE, stdin);
    }

    sem_wait(&sem_sending_data_ended);
    sending_data_ended = true;
    sem_post(&sem_sending_data_ended);

    receiving_requests_thread.join();
    replying_to_rexmit_requests_thread.join();

    set_of_rexmits.clear();
    delete[] reply_buffer;
    sem_destroy(&for_set_of_rexmits);
    sem_destroy(&sem_sending_data_ended);
    sem_destroy(&for_max_in_general);
    sem_destroy(&for_FIFO);
    if (FIFO.empty()) {
        delete[] data_portion;
    }
    else {
        for (uint8_t *pointer: FIFO) {
            delete[] pointer;
        }
    }

    CHECK_ERRNO(close(server_socket));
    CHECK_ERRNO(close(rexmiting_socket));
    CHECK_ERRNO(close(socket_fd));
    exit(0);
}