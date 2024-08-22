#!/bin/bash
#title           :generatieTextFile.sh
#description     :This script will generate randomly formatted textfile
#author		 :Jonathan Lejeune
#date            :20181031
#==============================================================================
pwd_avant=$PWD
cd `dirname $0`
WORKING_DIRECTORY=$PWD
cd $pwd_avant


error(){ 
    echo "$0 ERREUR : parametres invalides !" >&2 
    echo "utilisez l'option -h pour en savoir plus" >&2 
    exit 1 
} 

usage(){ 
    echo "Usage: $0 [options]" 
    echo "Ce script génére n fichiers de s Ko"
    
    echo "options obligatoires :"
    echo "	--type ou -t : type de données à générer : [lastfm, noodle, stereoprix]"
    echo "	--nbfile ou -n : nombre de fichiers à générer"
    echo "	--size ou -s : taille d'un fichier (en Ko)"
    
    echo "options facultatives :"
    echo "	--help ou -h : afficher l'aide"
} 


# -o : options courtes 
# -l : options longues 
options=$(getopt -o h,t:,n:,s: -l help,type:,nbfile:,size: -- "$@") 

# éclatement de $options en $1, $2... 
#set -- If no arguments follow this option, then the positional parameters are unset.  Otherwise, the positional parameters are set to the arguments,  even if some of them begin with a `-'. 
set -- $options 

type=""
nbfile=""
size=""

while true; do 
    case "$1" in 
    	-h|--help) usage 
            shift;; # on décale la liste des options de 1 
                    
        -t|--type)
            type=$(echo $2 | sed -e "s/'//g")
            shift 2
            ;;
        -n|--nbfile)
            nbfile=$(echo $2 | sed -e "s/'//g")
            shift 2
            ;;
        -s|--size)
            size=$(echo $2 | sed -e "s/'//g")
            shift 2
            ;;
        --) # fin des options 
            shift # on décale la liste des options de 1 
            break;; 
        *) error 
            shift;; # on décale la liste des options de 1 
    esac 
done

! [ -z "$type" ] && ! [ -z "$nbfile" ] && ! [ -z "$size" ] || error


CONFIG_DIR=$WORKING_DIRECTORY/generator_configs/textfile

if ! [ -d $CONFIG_DIR ]
then
	echo "le dossier $CONFIG_DIR n'existe pas"
	exit 1
fi

fileconfigtemplate=$CONFIG_DIR/$type.properties.template

if ! [ -e $fileconfigtemplate ]
then
	echo "$type n'est pas un type valide "
	error
fi

fileconfig="/tmp/$type.properties"

cat $fileconfigtemplate| sed -e "s/\(text.sizefichier=\).*/\1$size/g" | sed -e "s/\(text.nbfichier=\).*/\1$nbfile/g"  > $fileconfig


DATAGENERATOR_DIR=$WORKING_DIRECTORY/datagenerator

generator=$DATAGENERATOR_DIR/datagenerator.sh

$generator -t textfile -f $fileconfig








