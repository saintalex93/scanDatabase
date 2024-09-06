package br.com.totvs.scandatabase;

public class ScanDatabaseMain
{

    public static void main(
        final String[] args )
    {
        new DataBase().executeScanQuery( "DATABASE_NAME" );
    }

}
