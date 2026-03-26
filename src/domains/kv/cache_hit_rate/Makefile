CXX := g++
CXXFLAGS := -Wall -Wextra -O2 -pthread -std=c++11
LDFLAGS := -pthread

SRC := cache_calc.cpp
OBJ := $(patsubst %.cpp,build/%.o,$(SRC))
TARGET := cache_calc

$(shell mkdir -p build)

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(LDFLAGS) -o $@ $^

build/%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf build $(TARGET)
