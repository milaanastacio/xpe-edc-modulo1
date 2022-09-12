#!usr/bin/env bash

cd infrastructure


# Declara variavel para reutilizacao nas validacoes do diretorio

PACKAGE="package"

#Cria o diretorio e instala as dependencias da fincao lambda

if [ -d $PACKAGE]
then
    echo "O Diretorio " $PACKAGE" ja existe."
else
    echo "============================================="
    echo "Criando o diretorio" $PACKAGE"..."
    mkdir $PACKAGE
    echo "O Diretorio " $PACKAGE" foi criado." 
    echo "============================================="
fi    

# Declara variavel que localiza o requirements com as dependencias do projeto

FILE_REQUIREMENTS=../etl/lambda_requirements.txt

if [ -f $FILE_REQUIREMENTS]
then
    echo "============================================="
    echo "Instalando dependencias no" $FILE_REQUIREMENTS""
    pip install --target ./package -r $FILE_REQUIREMENTS
    echo "Dependencias instaladas com sucesso." 
    echo "============================================="
fi    

cd $PACKAGE


LAMBDA_FUNCTION=../../etl/lamdba_function.py

if [ -f $LAMBDA_FUNCTION]
then
    echo "============================================="
    echo "Copiando funcao Handler"
    cp $LAMBDA_FUNCTION .
    echo "Compactando arquivo lambda_funcition_payload.zip" 
    zip -r9 ../lambda_funcition_payload.zip . #compacta o pacote para o deploy
    echo "Arquivo compactado com sucesso!"
    echo "============================================="
fi  

cd ..