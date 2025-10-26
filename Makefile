CXX := g++
CXXFLAGS += -std=c++17 -Wall -Wextra -g -O2
CPPFLAGS += -I../cpp-libs
LDFLAGS += -luuid
LDLIBS += -pthread

ASAN ?= 0
ifeq ($(ASAN), 1)
  CXXFLAGS += -fsanitize=address -fsanitize=leak
endif

%: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MMD -MP -MF .$(@).d -o $@ $< $(LDFLAGS) $(LDLIBS)


-include .*.d
