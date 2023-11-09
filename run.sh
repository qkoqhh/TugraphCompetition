data_dir=$1
target_dir=$2

exe_file="TugraphCompetition-jar-with-dependencies.jar"

java -jar $exe_file $data_dir $target_dir

#class="com.antgroup.geaflow.case1and4.case1.Run"
#echo "CASE::$class"
#java -cp .:$exe_file $class  $data_dir $target_dir
#
#class="com.antgroup.geaflow.casetwo.CaseTwo"
#echo "CASE::$class"
#java -cp .:$exe_file $class  $data_dir $target_dir
#
#class="com.antgroup.geaflow.casethree.CaseThree"
#echo "CASE::$class"
#java -cp .:$exe_file $class  $data_dir $target_dir
#
#class="com.antgroup.geaflow.case1and4.case4.Run"
#echo "CASE::$class"
#java -cp .:$exe_file $class  $data_dir $target_dir
