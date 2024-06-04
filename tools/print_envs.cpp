#include <iostream>

int main(int argc, char**argv, char**envv) {
    for (;*envv; ++envv) {
        std::cout << *envv << '\n';
    }
}
