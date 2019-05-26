# Example of generate c++ code for file-format using thrift compiler

### File List
* build_carbondata_test.sh - Generate c++ code and compile
* carbondata_test.cpp - Parsing .carbonindex using generated c++ code

### Dependencies
* thrfit 0.9.3
* gcc 5.4
* pkg-config

### Usage
1. build the carbondata_test

```shell
    sh build_carbondata_test.sh
```

2. Check if an executable file "../target/carbondata_test" is generated.
```shell
    ls ../target/carbondata_test
```

3. Execute the carbondata_test wich a carbonindex file. Then will print the carbonindex version and carbondata file name.

```shell
    cd ../target
    ./carbondata_test <CARBONDATA_INDEXFILE>
```
