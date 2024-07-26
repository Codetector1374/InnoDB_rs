# InnoDB_rs

![](https://github.com/codetector1374/innodb_rs/actions/workflows/rust.yml/badge.svg)

InnoDB_rs is written to faciliate data recovery from accidentally deleted MySQL databases.
But also serves as an implementation of InnoDB's storage engine in Rust.

# Disclaimer
Project is currently under heavy development and in its early stages. 

# Building
Try building the tool in release mode unless you have a specific reason to not do so. 
The performance difference is non-trivial given Rust performs checked arithmetic 
in debug mode.

# Tools

## InnoDB Page Extractor (page_extractor)

Page extractor is designed to scan a file (or even entire block device) for any
InnoDB pages and extract them according to some rules.

Currently there are two extraction mode implemented:
- Index Mode
- Tablespace Mode

In *Index Mode* (default) the tool will examine all **index** pages, and extract 
them in a file that's named by their `index_id`. Output will be placed in `output_dir/FIL_PAGE_INDEX` with each file representing an index that the tool recovered.

In *Tablespace Mode* (`--by-tablespace`), it will try to store **ALL** page types
and store them in `output/BY_TABLESPACE`. Each file representing a table space. 
(`.ibd`) file.

See `--help` for more information
