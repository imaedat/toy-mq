CXX := g++
CXXFLAGS += -std=c++17 -Wall -Wextra -g -O2
CPPFLAGS += -I../cpp-libs
LDFLAGS +=
LDLIBS +=
# XXX
LDLIBS += -lstdc++fs
# XXX

ASAN ?= 0
ifeq ($(ASAN), 1)
  CXXFLAGS += -fsanitize=address -fsanitize=leak
endif

PROF ?= 0
ifeq ($(PROF), 1)
  CXXFLAGS += -pg
endif

.PHONY: cmd clean

all: broker

broker: LDLIBS += -lsqlite3 -pthread

cmd: cmd/publish cmd/subscribe

cmd/subscribe: LDLIBS += -luuid

%: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MMD -MP -MF .$(shell basename $@).d -o $@ $< $(LDFLAGS) $(LDLIBS)

clean:
	-rm -f broker cmd/publish cmd/subscribe .*.d

-include .*.d
