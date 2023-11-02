data_dir=$1
target_dir=$2

exe_file=TugraphCompetition-jar-with-dependencies.jar

class_list=(
	'com.antgroup.geaflow.case1and4.case1.Run' \
	'com.antgroup.geaflow.casetwo.CaseTwo' \
	'com.antgroup.geaflow.casethree.CaseThree' \
	'com.antgroup.geaflow.case1and4.case4.Run' \
	)

echo ${class_list[@]}

for class in ${class_list[@]};
do
echo "CASE::$class"
java -cp .:$exe_file $class  $data_dir $target_dir
done
