# MyRA Database Engine

A relational algebra based custom database engine.

## Compilation Instructions

We use `make` to compile all the files and creste the server executable. `make` is used primarily in Linux systems, so those of you who want to use Windows will probably have to look up alternatives (I hear there are ways to install `make` on Windows). To compile

`cd` into the soure directory (called `src`)

```
cd src
```

To compile

```
make clean
make
```

## To run

Post compilation, an executable names `server` will be created in the `src` directory

```
./server
```
