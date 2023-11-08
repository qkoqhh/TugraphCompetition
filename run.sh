#!/bin/bash
data_dir=$1
target_dir=$2

exe_file=TugraphCompetition-jar-with-dependencies.jar

class='com.antgroup.geaflow.case1and4.case1.Run'
echo "CASE::$class"
time java -cp .:$exe_file $class  $data_dir $target_dir

class='com.antgroup.geaflow.casetwo.CaseTwo'
echo "CASE::$class"
time java -cp .:$exe_file $class  $data_dir $target_dir

class='com.antgroup.geaflow.casethree.CaseThree'
echo "CASE::$class"
time java -cp .:$exe_file $class  $data_dir $target_dir

class='com.antgroup.geaflow.case1and4.case4.Run'
echo "CASE::$class"
time java -cp .:$exe_file $class  $data_dir $target_dir
