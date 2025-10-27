CXX := g++
CXXFLAGS += -std=c++17 -Wall -Wextra -g -O2
CPPFLAGS += -I../cpp-libs
LDFLAGS +=
LDLIBS += -luuid -lsqlite3 -pthread

ASAN ?= 0
ifeq ($(ASAN), 1)
  CXXFLAGS += -fsanitize=address -fsanitize=leak
endif

PROF ?= 0
ifeq ($(PROF), 1)
  CXXFLAGS += -pg
endif

%: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MMD -MP -MF .$(@).d -o $@ $< $(LDFLAGS) $(LDLIBS)


-include .*.d
