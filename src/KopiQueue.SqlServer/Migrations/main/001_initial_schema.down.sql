-- Removes everything version 1 created, jobs included.
DROP TABLE {{schema}}{{prefix}}queue;
DROP TABLE {{schema}}{{prefix}}leader;
DROP TABLE {{schema}}{{prefix}}job;
DROP TABLE {{schema}}{{prefix}}migration;
