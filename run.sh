data_dir=$1
target_dir=$2

exe_file=TugraphCompetition-jar-with-dependencies.jar

class_list=(com.antgroup.geaflow.case0and4.case1.Run
com.antgroup.geaflow.case1and4.case4.Run
com.antgroup.geaflow.casethree.CaseThree
com.antgroup.geaflow.casetwo.CaseTwo)

for class in class_list
do
java -cp .:$exe_file  data_dir target_dir
done