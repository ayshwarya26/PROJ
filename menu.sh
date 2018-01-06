#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[1;35m"` #Blue
    NUMBER=`echo "\033[1;33m"` #yellow
    FGRED=`echo "\033[33m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************H1B CASE STUDY***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1)${MENU} 1a  Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2)${MENU} 1b  Find top 5 job titles who are having highest avg growth in applications ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU} 2a  Which parts of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} 2b  find top 5 locations in the US who have got certified visa for each year ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5)${MENU} 3  Which industry(SOC_NAME) has the most number of Data Scientist positions? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6)${MENU} 4  Which top 5 employers file the most petitions each year? ${NORMAL}"    
    echo -e "${MENU}**${NUMBER} 7)${MENU} 5a  Find the most popular top 10 job positions for H1B visa applications for each year for all the applications? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8)${MENU} 5b  Find the most popular top 10 job positions for H1B visa applications for each yearfor only certified applications.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9)${MENU} 6  Find the percentage and the count of each case status on total applications for each year${NORMAL}" 
    echo -e "${MENU}**${NUMBER} 10)${MENU} 7  Create a bar graph to depict the number of applications for each year  ${NORMAL}"       
    echo -e "${MENU}**${NUMBER} 11)${MENU} 8  Find the average Prevailing Wage for each Job for each YearArrange the output in descending order ${NORMAL}"    
    echo -e "${MENU}**${NUMBER} 12)${MENU} 9  Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?
${NORMAL}"
     echo -e "${MENU}**${NUMBER} 13)${MENU} 10 Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?${NORMAL}"
     echo -e "${MENU}**${NUMBER} 14)${MENU} Export result for question no 10 to MySql database.${NORMAL}"
     echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;35m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

function getpinCodeBank(){
	echo "in getPinCodebank"
	echo $1
	echo $2
	#hive -e "Select * from AppData where PinCode = $1 AND Bank = '$2'"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
        option_picked "Is the number of petitions with Data Engineer job title increasing over time?"
        hive -e "select year,COUNT(job_title) from retail.h1b_final where job_title=='DATA ENGINEER' group by year order by year;"
        show_menu;
        ;;

        2) clear;
        option_picked "Find top 5 job titles who are having highest avg growth in application"
        pig /home/hduser/Proj/Proj1b.pig
        show_menu;
        ;;
            
        3) clear;
        option_picked "Which part of the US has the most Data Engineer jobs for each year?"
	bash /home/hduser/Proj/Proj2a.sh
        show_menu;
        ;;
	
        4) clear;
        option_picked "find top 5 locations in the US who have got certified visa for each year";
        echo "Enter the required year"
        read y
        echo "You've selected $y"
	    hive -e "select worksite,count(case_status) as c,year from retail.h1b_final where case_status='CERTIFIED' and year==$y group by worksite,year order by c desc limit 5;"
        show_menu;
        ;;
            
	    5) clear;
        option_picked "Which industry(SOC_NAME) has the most number of Data Scientist positions?";
        
                hive -e "select soc_name,COUNT(job_title) as c from retail.h1b_final where case_status='CERTIFIED' and job_title='DATA SCIENTIST' group by soc_name order by c desc limit 1;"
                show_menu;
                    ;;
                    
        6) clear;
         option_picked "Which top 5 employers file the most petitions each year?"
         echo "Enter the required year"
         read y
         echo "You've selected $y"
         hive -e "select employer_name,COUNT(case_status) as ca,year from retail.h1b_final where year==$y group by employer_name,year order by ca desc limit 5;";
        show_menu;
        ;;
        
        7) clear;
        option_picked "Find the most popular top 10 job positions for H1B visa applications for each year for all the applications?";
	 bash /home/hduser/Proj/Proj5a.sh
        show_menu;
        ;;
           
        8) clear;
        option_picked "Find the most popular top 10 job positions for H1B visa applications for each year or only certified applications";
	 bash /home/hduser/Proj/Proj5b.sh
        show_menu;
        ;;
        
       9) clear;
        option_picked  "Find the percentage and the count of each case status on total applications for each year."
	pig /home/hduser/Proj/Proj6.pig
        show_menu;
        ;;   
      10) clear;
        option_picked  "Create a bar graph to depict the number of applications for each year"
	bash /home/hduser/Proj/Proj7.sh
        show_menu;
        ;;   
      11) clear;
        option_picked  "Find the average Prevailing Wage for each Job for each YearArrange the output in descending order"
        echo -e "${MENU}Select Full Time or Part Time ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} Full Time ${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} Part Time ${NORMAL}"
        read n
	    case $n in
                1)	echo "FULL TIME SELECTED"
                        pig /home/hduser/Proj/Proj8Y.pig
                        ;;		
                    
                2) 	echo "PART TIME SELECTED"
                                       	
                    pig /home/hduser/Proj/Proj8N.pig
                    ;;   
                   *) echo "Please Select one among the option[1-5]";;
                esac
                show_menu;
                    ;;
      12) clear;
        option_picked  "Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?"
	 pig /home/hduser/Proj/Proj9.pig
        show_menu;
        ;;   
      13) clear;
        option_picked  "Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?"
	pig /home/hduser/Proj/Proj10.pig
        show_menu;
        ;;   
      14) clear;
        option_picked  "Export result for question no 10 to MySql databaser"
	sqoop export --connect jdbc:mysql://localhost/retail --username root --password 'aysh02' --table PROJ11 --update-mode  allowinsert --update-key Job   --export-dir /niit/part-r-00000 --input-fields-terminated-by '\t';

        show_menu;
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


