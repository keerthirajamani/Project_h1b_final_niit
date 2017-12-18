#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"`   #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
	masterus='root'
	masterpswd='thor'
	masterport='3160'
	masterdb='project'
    echo -e "${MENU}**********************APP MENU***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1)${MENU} Map Reduce (1a,2b,3,7)${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2)${MENU} Pig 1b,4,5a,9,10${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU} Hive 2a,5b,6,8${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} Sqoop 11${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}

show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
echo "** Here Comes Map Reduce!!!";
echo "Select A Query:" 
echo "1.Is the number of petitions with Data Engineer job title increasing over time ?"
echo "2.Find top 5 locations in the US who have got certified visa for each year? [certified]"
echo "3.Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified]"
echo "4.Create a bar graph to depict the number of applications for each year [All]"
	
read option;
 case $option in
                1)	hadoop fs -rm -r /output/one*
			hadoop jar '/home/hduser/Documents/onea.jar' onea /user/hive/warehouse/project.db/h1b_final /output/onea
			hadoop fs -cat /output/onea/p*
;;

		2)	hadoop fs -rm -r /output/two*
			hadoop jar '/home/hduser/Documents/twob.jar' twob /user/hive/warehouse/project.db/h1b_final /output/twob
			hadoop fs -cat /output/twob/p*
 ;;

		3) 	hadoop fs -rm -r /output/three*
			hadoop jar '/home/hduser/Documents/three.jar' three /user/hive/warehouse/project.db/h1b_final /output/three
			hadoop fs -cat /output/three/p*
					
;;
		4) 	hadoop fs -rm -r /output/seven*
			hadoop jar '/home/hduser/Documents/seven.jar' seven /user/hive/warehouse/project.db/h1b_final /output/seven
			hadoop fs -cat /output/seven/p*
			libreoffice seven_bar_graph.ods	
;;

		*) echo "Please Select one among the option[1-4]";;
                esac
                show_menu;
                    ;;


2)clear;
echo "** Here Comes Pig!!!";
echo "** 1.Find top 5 job titles who are having highest avg growth in applications.[ALL]";
echo "** 2.Which top 5 employers file the most petitions each year? - Case Status - ALL";
echo "** 3.Find the most popular top 10 job positions for H1B visa applications for each year for all the applications"
echo "** 4.Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?";
echo "** 5.Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?";

read pigopt;
case $pigopt in
                1)pig oneb.pig
                    ;;
		
		2)pig four.pig
                    ;;
		3)pig fivea.pig
                    ;;
		4)pig nine.pig
                    ;;
		5)pig ten.pig
                    ;;
		*) echo "Please Select one among the option[1-5]";;
                esac
                show_menu;
                    ;;
3)clear;
echo "** Here Comes Hive!!!";
echo "** 1.Which part of the US has the most Data Engineer jobs for each year?";
echo "** 2.Find the most popular top 10 job positions for H1B visa applications for each year and for only certified applications.";
echo "** 3.Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time."
echo "** 4.Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]";
read hiveopt
case $hiveopt in
		1)echo "Enter year from [2011-2016]"
			read op1
	hive -e "select worksite,count(*) as tot,year from project.h1b_final where job_title='DATA ENGINEER' AND year=$op1 group by year,worksite order by tot desc limit 10;";                   
			;;
                2)echo "Enter year from [2011-2016]"
			read op2
	hive -e "select job_title,count(case_status) as total,year from project.h1b_final where case_status='CERTIFIED' and year=$op2  group by job_title,year order by total desc limit 10;";
                    ;;
		3)hive -e "select a.case_status,count(a.case_status) as case_count,round((count(a.case_status)/total*100),2) as case_percent,a.year from project.h1b_final a,project.totalcount b group by a.case_status,b.total,a.year order by a.year;"
		libreoffice six_line_graph.ods 
                    ;;
		4)echo "Enter Job Position (Part time = N    Full time = Y)"
			read op4
 	hive -e "select year,job_title,avg(prevailing_wage) as totavg,full_time_position from project.h1b_final where full_time_position = '$op4' and (case_status='CERTIFIED' OR case_status='CERTIFIED-WITHDRAWN') group by job_title,year,full_time_position order by totavg desc;";
                    ;;	
		
		*) echo "Please Select one among the option[1-4]";;
                esac
                show_menu;
                    ;;
4)clear;
echo "** Here Comes Sqoop!!!";
echo "** Export result for question no 10 to MySql database";
	mysql -u root -p 'thor' -e 'select * from q10.qten';
	;;
 \n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi



done

