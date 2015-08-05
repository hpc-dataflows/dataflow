date

dst=/projects/visualization/cam/output
./bin/spark-submit --master spark://cc116:7077 /home/camc/code/dataflow/code/spark/aps_threshold.py --filelist /projects/visualization/cam/tilt_corrected_2-images/imagelist.txt --dst ${dst} -p ${1} -s ${2}
name=`ls -d ${dst}/output-pct${1}-sigma${2}*`
~/code/nvisus/build/server-8fd2107-17Jun2015/visusconvert --create ${name}.idx --box "0 1882 0 1900 0 130" --fields "data 1*uint8"
for ((i=0;i<131;i++)); do j=$i; while [ ${#j} -lt 3 ]; do j=0${j}; done; files="${files} --paste ${name}/img-${j}.tif --destination-box \"0 1882 0 1900 ${i} ${i}\""; done
cmd="~/code/nvisus/build/server-8fd2107-17Jun2015/visusconvert --import /dev/null --dims \"1883 1901 131\" --dtype 1*float32 ${files} --cast 1*uint8 --export ${name}.idx --box \"0 1882 0 1900 0 130\" --field data"
#echo $cmd
eval $cmd

date
