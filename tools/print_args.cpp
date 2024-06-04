#include <iostream>

int main(int argc, char**argv, char**envv) {
    for (;*argv; ++argv) {
        std::cout << *argv << '\n';
    }
}
