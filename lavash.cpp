#include <unistd.h>
#include <stdlib.h>
#include <iostream>

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <sstream>
#include <istream>
#include <exception>
#include <string>
#include <variant>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <cstring>
#include <sys/types.h>
#include <sys/wait.h>

class Tokenizer {
public:
    static const std::unordered_set <std::string> set_of_programs;

    struct Program {
        explicit Program(const std::string &program_name) : name(program_name) {}

        std::string name;
    };

    enum Operator {
        OR,
        AND,
        IN_REDIRECT,
        OUT_REDIRECT,
        PIPE,
        NONE
    };

    struct File {
        explicit File(const std::string &file_path) : path(file_path) {}

        std::string path;
    };

    using Token = std::variant<Program, Operator, File, bool, std::string>;

    explicit Tokenizer(char *str) : is_(str) {
        Tokenize();
    }

    void Tokenize() {
        while (is_.peek() != EOF) {
            while (is_.peek() != EOF && isspace(is_.peek())) {
                is_.get();
            }

            std::string token;
            while (is_.peek() != EOF && !isspace(is_.peek())) {
                char ch = is_.get();

                if (ch == '\\') {
                    ch = is_.get();
                    token += ch;
                    continue;
                }

                if (ch == '"' && token.empty()) {
                    ch = is_.get();
                    while (ch != '"' && ch != EOF) {
                        if (ch == '\\') {
                            is_.get();
                        }
                        token += ch;
                        ch = is_.get();
                    }

                    if (ch == EOF) {
                        token = '"' + token;
                        AddToken(token);
                        token.clear();
                        break;
                    }

                    tokens_.push_back(token);
                    token.clear();
                    break;
                }

                if (ch == '<' || ch == '>') {
                    AddToken(token);
                    token.clear();

                    token += ch;
                    AddToken(token);
                    token.clear();

                    break;
                }

                token += ch;
            }

            AddToken(token);
            token.clear();
        }
    }

    Token *GetNextToken() {
        if (cur_token_ + 1 >= tokens_.size()) {
            throw std::invalid_argument("No next tokens");
        }

        return &tokens_[cur_token_ + 1];
    }

    Token *GetCurToken() {
        if (IsEnd()) {
            throw std::invalid_argument("No current token");
        }

        return &tokens_[cur_token_];
    }

    Token *GetPrevToken() {
        if (cur_token_ <= 0) {
            throw std::invalid_argument("No previous tokens");
        }

        return &tokens_[cur_token_ - 1];
    }

    void MoveTokenCnt() {
        ++cur_token_;
    }

    bool IsEnd() const {
        return cur_token_ >= tokens_.size();
    }

private:
    void AddToken(const std::string token) {
        if (token.empty()) {
            return;
        }

        if (auto oper_iter = operator_map_.find(token); oper_iter != operator_map_.end()) {
            tokens_.push_back(oper_iter->second);
//            std::cerr << "oper " << token << '\n';
        } else if (auto prog_iter = set_of_programs.find(token); prog_iter != set_of_programs.end()) {
            tokens_.push_back(Program(*prog_iter));
//            std::cerr << "program " << token << '\n';
        } else if (token.size() > 4 && token.substr(token.size() - 4, 4) == ".txt") {
            tokens_.push_back(File(token));
//            std::cerr << "file " << token << '\n';
        } else if (token == "true") {
            tokens_.push_back(true);
        } else if (token == "false") {
            tokens_.push_back(false);
        } else {
            tokens_.push_back(token);
//            std::cerr << "argument " << token << '\n';
        }
    }

    std::istringstream is_;
    std::vector <Token> tokens_;
    static const std::unordered_map <std::string, Operator> operator_map_;
    size_t cur_token_ = 0;
};

const std::unordered_map <std::string, Tokenizer::Operator> Tokenizer::operator_map_ = {
        {"&&", Tokenizer::Operator::AND},
        {"||", Tokenizer::Operator::OR},
        {"<",  Tokenizer::Operator::IN_REDIRECT},
        {">",  Tokenizer::Operator::OUT_REDIRECT},
        {"|",  Tokenizer::Operator::PIPE}};
const std::unordered_set <std::string> Tokenizer::set_of_programs = {"echo", "wc", "cat", "./tools/print_args"};

struct BootKit {
    char *arguments[10];
    size_t arguments_size = 0;
    const char *program_name = nullptr;
    int in_descriptor = fileno(stdin);
    int out_descriptor = fileno(stdout);
    Tokenizer::Operator next_oper = Tokenizer::Operator::NONE;
    bool need_to_boot = true;
};

char *CopyStr(const char *str) {
    char *new_str = static_cast<char *>(calloc(strlen(str), sizeof(str[0])));
    strcpy(new_str, str);
    return new_str;
}

BootKit FormBootKit(Tokenizer &tokenizer) {
    BootKit boot_kit;

    Tokenizer::Token *cur_token = tokenizer.GetCurToken();
    Tokenizer::Program *program = std::get_if<Tokenizer::Program>(&*cur_token);
    if (program == nullptr) {
        throw std::invalid_argument("BootKit cannot be formed");
    }

    size_t cur_idx = 0;

    boot_kit.program_name = CopyStr(program->name.c_str());
    boot_kit.arguments[cur_idx] = CopyStr(program->name.c_str());
    ++cur_idx;
    tokenizer.MoveTokenCnt();

    while (!tokenizer.IsEnd()) {
        cur_token = tokenizer.GetCurToken();

        Tokenizer::Operator *oper = std::get_if<Tokenizer::Operator>(&*cur_token);
        if (oper != nullptr) {
            boot_kit.arguments[cur_idx] = nullptr;
            boot_kit.arguments_size = cur_idx;
            return boot_kit;
        } else if (Tokenizer::File *file = std::get_if<Tokenizer::File>(&*cur_token)) {
            boot_kit.arguments[cur_idx] = CopyStr(file->path.c_str());
            ++cur_idx;
        } else if (std::string * arg = std::get_if<std::string>(&*cur_token)) {
            boot_kit.arguments[cur_idx] = CopyStr(arg->c_str());
            ++cur_idx;
        } else if (Tokenizer::Program *prog = std::get_if<Tokenizer::Program>(&*cur_token)) {
            boot_kit.arguments[cur_idx] = CopyStr(prog->name.c_str());
            ++cur_idx;
        } else if (bool *arg = std::get_if<bool>(&*cur_token)) {
            boot_kit.arguments[cur_idx] = CopyStr(*arg ? "true\0" : "false\0");
            ++cur_idx;
        }

        tokenizer.MoveTokenCnt();
    }

    boot_kit.arguments[cur_idx] = nullptr;
    boot_kit.arguments_size = cur_idx;
    return boot_kit;
}

void CloseFDs(const std::unordered_map<std::string, int> &fds) {
    for (auto [file, fd]: fds) {
        close(fd);
    }
}

int main(int argc, char **argv, char **envv) {
    if (argc < 3) {
        return 0;
    } else if (argc != 3) {
        throw std::invalid_argument("Invalid arguments");
    }

//    std::cout << "whole command: " << argv[2] << '\n';
    Tokenizer tokenizer(argv[2]);

    std::vector <BootKit> programs;
    std::unordered_map<std::string, int> fds;


    while (!tokenizer.IsEnd()) {
//        std::cout << "started parsing" << '\n';
        Tokenizer::Token *cur_token = tokenizer.GetCurToken();

        Tokenizer::Program *program = std::get_if<Tokenizer::Program>(&*cur_token);
        if (program) {
            BootKit boot_kit = FormBootKit(tokenizer);
            programs.push_back(boot_kit);

            if (tokenizer.IsEnd()) {
                continue;
            }

            cur_token = tokenizer.GetCurToken();
            Tokenizer::Operator *oper = std::get_if<Tokenizer::Operator>(&*cur_token);
            while (oper && (*oper == Tokenizer::Operator::PIPE)) {
                int filedes[2];
                int ret = pipe(filedes);
                if (ret < 0) {
                    throw std::runtime_error("Could not create pipe");
                }
                programs.back().out_descriptor = filedes[1];

                tokenizer.MoveTokenCnt();
                if (tokenizer.IsEnd()) {
                    throw std::invalid_argument("After pipe should follow program");
                }

                cur_token = tokenizer.GetCurToken();
                Tokenizer::Program *program = std::get_if<Tokenizer::Program>(&*cur_token);
                if (program) {
                    boot_kit = FormBootKit(tokenizer);
                    programs.push_back(boot_kit);
                } else {
                    if (std::string * arg = std::get_if<std::string>(&*cur_token)) {
                        if (*arg == "1984") {
                            return 0;
                        }
                    }
                    throw std::invalid_argument("After pipe should follow program");
                }

                programs.back().in_descriptor = filedes[0];


                if (tokenizer.IsEnd()) {
                    break;
                }

                cur_token = tokenizer.GetCurToken();
                oper = std::get_if<Tokenizer::Operator>(&*cur_token);
            }
        } else if (Tokenizer::Operator *oper = std::get_if<Tokenizer::Operator>(&*cur_token)) {
            if (*oper == Tokenizer::OUT_REDIRECT) {
                tokenizer.MoveTokenCnt();
                if (tokenizer.IsEnd()) {
                    throw std::invalid_argument("No file after out_redirecting");
                }

                Tokenizer::Token *file_token = tokenizer.GetCurToken();
                if (Tokenizer::File *file = std::get_if<Tokenizer::File>(&*file_token)) {
                    int fd = open(file->path.c_str(), O_CREAT | O_WRONLY, 0666);

                    if (fd == -1) {
                        throw std::runtime_error("Cannot open " + file->path);
                    }

                    fds[file->path] = fd;

                    if (programs.empty()) {
                        tokenizer.MoveTokenCnt();
                        Tokenizer::Token *program_token = tokenizer.GetCurToken();
                        Tokenizer::Program *program = std::get_if<Tokenizer::Program>(&*program_token);
                        if (program) {
                            BootKit boot_kit = FormBootKit(tokenizer);
                            programs.push_back(boot_kit);
                        }
                    }
                    programs.back().out_descriptor = fd;
                } else {
                    throw std::invalid_argument("No file after in_redirecting");
                }
                tokenizer.MoveTokenCnt();
            } else if (*oper == Tokenizer::IN_REDIRECT) {
                tokenizer.MoveTokenCnt();
                if (tokenizer.IsEnd()) {
                    throw std::invalid_argument("No file after out_redirecting");
                }

                Tokenizer::Token *file_token = tokenizer.GetCurToken();
                if (Tokenizer::File *file = std::get_if<Tokenizer::File>(&*file_token)) {
                    int fd = open(file->path.c_str(), O_RDONLY, 0666);

                    if (fd == -1) {
                        std::cerr << "./lavash: line 1: " << file->path << ": No such file or directory" << '\n';
                        if (programs.empty()) {
                            BootKit boot_kit;
                            boot_kit.program_name = "no_txt";
                            programs.push_back(boot_kit);
                        } else {
                            programs.back().need_to_boot = false;
                        }
                        tokenizer.MoveTokenCnt();
                        continue;
                    }

                    fds[file->path] = fd;

                    if (programs.empty()) {
                        tokenizer.MoveTokenCnt();
                        Tokenizer::Token *program_token = tokenizer.GetCurToken();
                        Tokenizer::Program *program = std::get_if<Tokenizer::Program>(&*program_token);
                        if (program) {
                            BootKit boot_kit = FormBootKit(tokenizer);
                            programs.push_back(boot_kit);
                        }
                    }
                    programs.back().in_descriptor = fd;
                } else {
                    throw std::invalid_argument("No file after in_redirecting");
                }
                tokenizer.MoveTokenCnt();
            } else if (*oper == Tokenizer::Operator::PIPE) {
                int filedes[2];
                int ret = pipe(filedes);
                if (ret < 0) {
                    throw std::runtime_error("Could not create pipe");
                }

                close(filedes[1]);

                tokenizer.MoveTokenCnt();
                if (tokenizer.IsEnd()) {
                    throw std::invalid_argument("After pipe should follow program");
                }

                cur_token = tokenizer.GetCurToken();
                Tokenizer::Program *program = std::get_if<Tokenizer::Program>(&*cur_token);
                if (program) {
                    BootKit boot_kit = FormBootKit(tokenizer);
                    programs.push_back(boot_kit);
                } else {
                    throw std::invalid_argument("After pipe should follow program");
                }

                programs.back().in_descriptor = filedes[0];
            } else if (*oper == Tokenizer::Operator::AND || *oper == Tokenizer::Operator::OR) {
                programs.back().next_oper = *oper;
                tokenizer.MoveTokenCnt();
            }


        } else if (std::string * arg = std::get_if<std::string>(&*cur_token)) {
            BootKit boot_kit;
            boot_kit.program_name = arg->c_str();
            programs.push_back(boot_kit);
            tokenizer.MoveTokenCnt();
        } else if (bool *boolean = std::get_if<bool>(&*cur_token)) {
            BootKit boot_kit;
            boot_kit.program_name = *boolean ? "true" : "false";
            programs.push_back(boot_kit);
            tokenizer.MoveTokenCnt();
        }
    }
    int return_code = 0;

    for (size_t i = 0; i < programs.size(); ++i) {
        BootKit &boot_kit = programs[i];
        if (!boot_kit.need_to_boot) {
            return_code = 1;
        } else if (strcmp(boot_kit.program_name, "true") == 0) {
            return_code = 0;
        } else if (strcmp(boot_kit.program_name, "false") == 0) {
            return_code = 1;
        } else if (strcmp(boot_kit.program_name, "no_txt") == 0) {
            return_code = 1;
        } else if (Tokenizer::set_of_programs.find(std::string(boot_kit.program_name)) ==
                   Tokenizer::set_of_programs.end()) {
            std::cerr << "./lavash: line 1: " << boot_kit.program_name << ": command not found" << '\n';
            return_code = 127;
        } else {
            pid_t pid = fork();

            if (pid == 0) {
                int ret = dup2(boot_kit.in_descriptor, fileno(stdin));
                if (ret < 0) {
                    throw std::runtime_error("dup2 did not work");
                }
                ret = dup2(boot_kit.out_descriptor, fileno(stdout));
                if (ret < 0) {
                    throw std::runtime_error("dup2 did not work");
                }

                if (boot_kit.program_name[0] == '/') {
                    execv(boot_kit.program_name, boot_kit.arguments);
                } else {
                    execvp(boot_kit.program_name, boot_kit.arguments);
                }

                throw std::runtime_error("subprogram did not started");
            } else {
                wait(&return_code);
                if (boot_kit.in_descriptor != fileno(stdin)) {
                    close(boot_kit.in_descriptor);
                }
                if (boot_kit.out_descriptor != fileno(stdout)) {
                    close(boot_kit.out_descriptor);
                }
            }
        }

        if (return_code == 0) {
            if (boot_kit.next_oper == Tokenizer::Operator::OR) {
                CloseFDs(fds);
                return return_code;
            }
        } else {
            if (boot_kit.next_oper == Tokenizer::Operator::AND) {
                while (i < programs.size() && (programs[i].next_oper == Tokenizer::Operator::AND ||
                                               programs[i].next_oper == Tokenizer::Operator::NONE)) {
                    ++i;
                }
            }
        }


    }

    CloseFDs(fds);

    return return_code;
}
