# Game-of-Life


A Go implementation of Conway’s Game of Life, developed as part of a university coursework project at the University of Bristol.

This project simulates the evolution of the Game of Life on a 2D image matrix and includes a test suite and visualiser based on limited provided skeleton code.

Due to the consideration of information protection, the task instruction will not be demonstrated here according to university policy.

---

## Overview


The **Game of Life** is a cellular automaton devised by mathematician John Horton Conway. It operates on a binary image where each cell is either “alive” or “dead”, and updates over time according to specific rules based on adjacent neighbours.

Your implementation processes these rules to evolve the state of the game over a number of turns.

---

## Features


- Simulates Conway’s Game of Life on a 2D grid using Go
- Supports reading and evolving binary image states
- Correct implementation of neighbor-based evolution rules
- Includes automated test suite to validate correctness
- Visualisation using SDL (via provided skeleton code)

---

## Tech Stack


- **Language:** Go (Golang)  
- **Testing:** Go test (`_test.go` files used to verify correctness)  
- **Visualiser:** SDL library for optional graphical output (provided)  
- **Version Control:** Git

---

## Project Structure



There's two implementation of this project. One is in distribution style and the other is in parallel style.

Both of them have their own readme file for simple usage and instruction. Their own licenses are included as well.
