lavash: lavash.cpp
	$(CXX) $^ -Wall -Werror -o $@

tools/%: tools/%.cpp
	$(CXX) $^ -Wall -Werror -o $@

tools: tools/print_args tools/print_envs

test: lavash test.py tools
	python3 test.py
