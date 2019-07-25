# BlockLayoutMigration
This utility will help us to migrate HDFS block from old layout (hadoop-2.x) to new Layout (hadoop-3.x) in LFS.

We saw all blocks are missing or corrupt after hadoop-3.x upgrade. This issue is caused by manual data copy, by proceeded causion with an incomplete upgrade. 

This utility will move blocks into appropriate subdirs layout and delete stale meta and block files from LFS.

## Usage

#nohup <JAVA_HOME>/bin/java BlockLayoutMigration \<2|3\> \<comma-separated-datadir\> \<block-pool-id\> [mover-threads]  &>/vat/tmp/block-layout-migration.log &


NOTE

If you are migrating blocks from hadoop-3.x to hadoop-2.x, then pass "2" in the args[0]

If you are migrating blocks from hadoop-2.x to hadoop-3.x, then pass "3" in the args[0]

By default, 10 mover threads. You can increase threads for parallelization.

## Download, Compile and Run

### To compile,
<JAVA_HOME>/bin/javac BlockLayoutMigration.java

### To run,
#nohup <JAVA_HOME>/bin/java BlockLayoutMigration \<comma-separated-datadir\> \<block-pool-id\> [mover-threads]  &>/vat/tmp/block-layout-migration.log &



