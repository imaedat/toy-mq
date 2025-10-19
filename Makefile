CXX := g++
CXXFLAGS += -std=c++17 -Wall -Wextra -g -O2
CPPFLAGS += -I../cpp-libs
LDFLAGS +=
LDLIBS += -pthread

%: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MMD -MP -MF .$(@).d -o $@ $< $(LDFLAGS) $(LDLIBS)


-include .*.d
