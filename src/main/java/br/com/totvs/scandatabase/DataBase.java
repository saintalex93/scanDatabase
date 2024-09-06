package br.com.totvs.scandatabase;

import static java.util.Arrays.asList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Colar a saída do console no excel, selecionar a primeira coluna e ir em Dados
 * -> Texto para Colunas -> Delimitado -> Selecionar em delimitadores "Outros" e
 * colocar um pipe "|"
 */

public class DataBase
{

    private static final List<String> IGNORED_TABLES = asList( "databasechangeloglock", "databasechangelog", "spatial_ref_sys", "revinfo" );

    private final String user = "myUser";
    private final String password = "myPassword";

    public Connection connect(
        final String database )
    {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection( getUrl( database ), user, password );
        } catch( final SQLException e ) {
            System.out.println( e.getMessage() );
        }

        return conn;
    }

    private String getUrl(
        final String database )
    {
        return "jdbc:postgresql://nlg32v/" + database;
    }

    public void executeScanQuery(
        final String database )
    {
        try {
            final Connection connection = connect( database );
            final Statement tableSt = connection.createStatement();

            for( final String tableName : getTableNames( database ) ) {
                final ResultSet executeQuery = tableSt.executeQuery( "SELECT * FROM " + tableName );
                final ResultSetMetaData rsmd = executeQuery.getMetaData();
                final int columnCount = rsmd.getColumnCount();
                while( executeQuery.next() ) {
                    if( executeQuery.getRow() == 1 ) {
                        System.out.println( "" );
                        System.out.println( tableName );
                        for( int i = 1; i <= columnCount; i++ ) {
                            final String name = rsmd.getColumnName( i );
                            System.out.print( name + " | " );
                        }
                        System.out.println( "" );
                    }
                    for( int i = 1; i <= columnCount; i++ ) {
                        final String name = rsmd.getColumnName( i );
                        final String value = executeQuery.getString( name );
                        if( value == null ) {
                            System.out.print( "  | " );
                            continue;
                        }
                        System.out.print( executeQuery.getString( name ).replace( "\n", "" ).replace( "\r", "" ) + " | " );
                    }
                    System.out.println( "" );
                }
                executeQuery.close();
            }
            tableSt.close();
        } catch( final Exception e ) {
            e.printStackTrace();
        }
    }

    private List<String> getTableNames(
        final String database )
    {
        final Connection connection = connect( database );
        final List<String> tableNames = new ArrayList<>();
        try {
            final Statement st = connection.createStatement();
            final ResultSet rs = st.executeQuery( "SELECT * FROM pg_catalog.pg_tables where tableowner <> 'postgres'" );
            while( rs.next() ) {
                if( ! rs.getString( "tablename" ).contains( "_aud" ) ) {
                    tableNames.add( rs.getString( "tablename" ) );
                }
            }
            rs.close();
            st.close();
        } catch( final Exception e ) {
        }
        tableNames.removeAll( IGNORED_TABLES );
        return tableNames;
    }

}
