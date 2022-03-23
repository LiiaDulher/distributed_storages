# Task 2
## Distributed map
## Team: [Liia Dulher](https://github.com/LiiaDulher)
### Usage
First start Hazelcast.<br>
For parts from 1 to 4 use scripts from file main.py.
````
main.py part_number
````
#### Part 1
Script for checking information distribution on nodes.
#### Part 2
Script for checking map with locks work with 3 clients.

##### Important!
Make changes in <b>hazelcast.xml</b> file before running next parts.
````
<hazelcast>
    ...
    <queue name="bounded_queue">
        <max-size>101</max-size>
    </queue>
    ...
</hazelcast>
````

#### Part 3
Script for checking writing into full bounded queue.
#### Part 4
Script for checking reading from queue with two clients.
