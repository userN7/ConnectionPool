package com.company;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Executor;


class ConnectionWrapper implements Connection //Простая реализация интерфейса java.sql.Connection
        // Реализовали только защищенное соединение через метод Close(), чтобы при попытки закрыть соединение, оно бы пыталось
        // вернуть его в пул, а так же createStatement(), который возвращает результат работы из реализации этого метода,
        // который предоставляет драйвер, в нашем случае из класса SQLServerConnection, чтобы можно было делать тестовые запросы
{
    private Connection m_connection;

    private boolean isConnCanBeClosed;

    public Connection getRealConnection() {return m_connection;
    }

    public ConnectionWrapper(Connection connection) {
        this.m_connection = connection;
        this.isConnCanBeClosed = false;
    }

      /*  public void SetConnCanBeClosed(boolean flag)
        {
            connCanBeClosed=flag;
        }*/

    public void close() throws SQLException {//Защита от закрытия
        if (this.isConnCanBeClosed)
            this.m_connection.close();

    }


    public Statement createStatement() throws SQLException {
        return m_connection.createStatement();
    }


    public PreparedStatement prepareStatement(String s)  {
        return null;
    }


    public CallableStatement prepareCall(String s)  {
        return null;
    }


    public String nativeSQL(String s)  {
        return null;
    }


    public void setAutoCommit(boolean b)  {

    }


    public boolean getAutoCommit()  {
        return false;
    }


    public void commit()  {

    }


    public void rollback()  {

    }




    public boolean isClosed()  {
        return false;
    }


    public DatabaseMetaData getMetaData()  {
        return null;
    }


    public void setReadOnly(boolean b)  {

    }


    public boolean isReadOnly()  {
        return false;
    }


    public void setCatalog(String s)  {

    }


    public String getCatalog()  {
        return null;
    }


    public void setTransactionIsolation(int i)  {

    }


    public int getTransactionIsolation()  {
        return Connection.TRANSACTION_NONE;
    }


    public SQLWarning getWarnings()  {
        return null;
    }


    public void clearWarnings()  {

    }


    public Statement createStatement(int i, int i1)  {
        return null;
    }


    public PreparedStatement prepareStatement(String s, int i, int i1)  {
        return null;
    }


    public CallableStatement prepareCall(String s, int i, int i1)  {
        return null;
    }


    public Map<String, Class<?>> getTypeMap()  {
        return null;
    }


    public void setTypeMap(Map<String, Class<?>> map)  {

    }


    public void setHoldability(int i)  {

    }


    public int getHoldability()  {
        return 0;
    }


    public Savepoint setSavepoint()  {
        return null;
    }


    public Savepoint setSavepoint(String s)  {
        return null;
    }


    public void rollback(Savepoint savepoint)  {

    }


    public void releaseSavepoint(Savepoint savepoint)  {

    }


    public Statement createStatement(int i, int i1, int i2)  {
        return null;
    }


    public PreparedStatement prepareStatement(String s, int i, int i1, int i2)  {
        return null;
    }


    public CallableStatement prepareCall(String s, int i, int i1, int i2)  {
        return null;
    }


    public PreparedStatement prepareStatement(String s, int i)  {
        return null;
    }


    public PreparedStatement prepareStatement(String s, int[] ints)  {
        return null;
    }


    public PreparedStatement prepareStatement(String s, String[] strings)  {
        return null;
    }


    public Clob createClob()  {
        return null;
    }


    public Blob createBlob()  {
        return null;
    }


    public NClob createNClob()  {
        return null;
    }


    public SQLXML createSQLXML()  {
        return null;
    }


    public boolean isValid(int i)  {
        return false;
    }


    public void setClientInfo(String s, String s1)  {

    }


    public void setClientInfo(Properties properties) {

    }


    public String getClientInfo(String s)  {
        return null;
    }


    public Properties getClientInfo()  {
        return null;
    }


    public Array createArrayOf(String s, Object[] objects)  {
        return null;
    }


    public Struct createStruct(String s, Object[] objects)  {
        return null;
    }


    public void setSchema(String s)  {

    }


    public String getSchema()  {
        return null;
    }


    public void abort(Executor executor)  {

    }


    public void setNetworkTimeout(Executor executor, int i)  {

    }


    public int getNetworkTimeout()  {
        return 0;
    }



    public <T> T unwrap(Class<T> aClass)  {
        return null;
    }


    public boolean isWrapperFor(Class<?> aClass)  {
        return false;
    }


}

//Простая реализация пула соединения. Для синхронизации между потоками будем хранить состояние пула в экземпляре класса State.
// В нем будут хранится массивы с выданными соединениями и бездействующими соединениями. Первично заполняем пул бездействующих соединений
// на величину Math.min(initConnCount,maxIdleConnection) Когда поток хочет получить соединение из пула, то он вызывает getConnection(),
// который запускает отдельный поток в котором ждет пока в пуле использованных соединений не появится место (которое ограничено),
// и если место появляется,то берет соединение из бездействующих соединений или создаёт новое если таких нет,
// и который блокирует состояние пула state.
// Соединения выдаются в защищённой оболочке ConnectionWrapper, которая является реализацией интерфейса java.sql.Connection,
// и которая содержит соединение и флаг isConnCanBeClosed запрещающий закрывать соединение. Если соединение не успело выдаться в срок,
// отдельный поток в котором ожидалось соединение заканчивает работу, и если он не получил соединение, то выдается исключение.
// Для возвращения в пул используется метод releaseConnection, который складывает соединения в пул, в массив бездействующих соединений,
// если там есть место (оно ограничено), либо закрывает соединение. Так же блокирует состояние через state
// Так же при создании пула создается задача CheckIdleConnTask, которая будет периодически запускаться и чистить пул бездействующий
// выше срока соединений. Так же блокирует состояние через state.

class ConnectionPool {

private final class State
        {
             final List<availableConn> availableConnections ;
             final List<Connection> usedConnections;
             public State()
            {
                availableConnections = new ArrayList<>();
                usedConnections = new ArrayList<>();
            }
        }

    //класс для хранения объекта в доступных на выдачу соединений
    private class availableConn
    {
        private Connection conn;
        private long lifeTimeStamp;
        public availableConn(Connection conn)
        {
            this.conn = conn;
            this.lifeTimeStamp = System.currentTimeMillis()+millisToFreeIdleConn;
        }
        public long GetLifeTimeStamp(){return lifeTimeStamp;}
        public Connection GetConnection(){return conn;}

    }

    private class CheckForAvailableConn {
        private Connection openedConn=null;
        private boolean isActive=true;
        Thread checker;

        public Connection getConn() {return openedConn;}
        public void StartCheck()
        {
            checker.start();
        }
        public void StopCheck()
        {
            isActive=false;
        }
        CheckForAvailableConn(ConnectionPool connectionPool){


            Runnable r = ()-> {


                    while ( isActive&&(openedConn == null)){//пока задача активна нам не выдадут соединение пробуем его получить
                        synchronized (state) {
                            /* System.out.printf("Поток %s ждет соединение, занял state и заснул\n", Thread.currentThread().getName());
                    try {
                        Thread.sleep(100000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }*/
                            //Смотрим не превышен лимит на открытые соединения
                            if (maxActiveConnection > state.usedConnections.size()) {
                                //если есть доступные соединения в пулы то выдаем
                                Connection con = null;
                                /* System.out.printf("Создано новое соединение для пула и положено в массив открытых соединения для потока %s\n", Thread.currentThread().getName());
                                    printCurrentConn();*/
                                if (!state.availableConnections.isEmpty()) {
                                    con = state.availableConnections.get(state.availableConnections.size() - 1).GetConnection();
                                    state.availableConnections.remove(state.availableConnections.size() - 1);

                                   /* System.out.printf("Выдано соединение из пула бездействующих соединений для потока %s\n", Thread.currentThread().getName());
                                    printCurrentConn();*/

                                }
                                //иначе создаем новое
                                else try {
                                    con = createConnection();
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                                state.usedConnections.add(con);
                                openedConn = con;
                            }
                        }



                    }
                if (!isActive&&openedConn != null) //если получили соединение в итоге, но оно уже не нужно то пробуем положить его обратно

                {
                    try {
                        connectionPool.releaseConnection(openedConn);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

            };
            checker = new Thread(r, "checkerFor_" + Thread.currentThread().getName());


        }



    }
    //с помощью этой задачи будем проверять пришел ли срок удаления бездействующего соединения
    private class CheckIdleConnTask extends TimerTask {

        @Override
        public void run() {
           System.out.printf("\nЗадача очистки бездействующих соединений запущена\n" +
                    "Время %1$tT %1$td.%1$tm.%1$tY\n", new Date());
           printCurrentConn();

            synchronized (state) {
              /*  System.out.printf("Поток %s начал очистку соединений, занял state и заснул\n", Thread.currentThread().getName());
                try {
                    Thread.sleep(100000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
                for (int i = 0; i < state.availableConnections.size(); i++) {
                    try {
                        //Если соединение просрочено то закрываем и удаляем из пула
                        if (state.availableConnections.get(i).GetLifeTimeStamp() < System.currentTimeMillis()) {
                            if (state.availableConnections.get(i).GetConnection() != null)
                                state.availableConnections.get(i).GetConnection().close();
                                state.availableConnections.remove(state.availableConnections.get(i));
                              System.out.println("Очистили бездействующее соединение");
                            printCurrentConn();
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                }
            }





        }


    }

    private int maxActiveConnection;
    private int maxIdleConnection;
    private String URL;
    private String USER;
    private String PASSWORD;
    private long millisToWaitFreeConnection;
    private long millisToFreeIdleConn;
    long millisTimeToWaitBeforeNextTry = 10;//мс Сколько будем ждать перед новой получить соединение
    private final State state;// где будем хранить списки открытых и бездействующих соединений, нужен для синхронизации между потоками

    private boolean isDeamonCheckIdleConnTask;//Закрывать ли поток очистки бездействующих соединений после работы основного потока

    //создаем пул с указаными параметрами
    public ConnectionPool(String driver, String url, String user, String password, int maxActiveConnection, int maxIdleConnection, long millisToWaitFreeConnection, long millisToFreeIdleConn )
            throws SQLException {

        try {
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.URL = url;
        this.USER = user;
        this.PASSWORD = password;
        this.maxActiveConnection= maxActiveConnection;
        this.maxIdleConnection= maxIdleConnection;
        this.millisToWaitFreeConnection = millisToWaitFreeConnection;
        this.millisToFreeIdleConn = millisToFreeIdleConn;

        this.state = new State();
        //первично заполняем пул
        int initConnCount = 5;
        System.out.println("Первичное заполнение пула");
        initConnCount= Math.min(initConnCount,maxIdleConnection);//Проверяем больше ли лимита на бездействующие соединения
        for (int count = 0; count < initConnCount; count++) {
            state.availableConnections.add( new availableConn( this.createConnection()));
        }
        printCurrentConn();
        // Запускаем задание на проверку бездействующих соединений
        isDeamonCheckIdleConnTask = false;
        Timer timer = new Timer(isDeamonCheckIdleConnTask);
        TimerTask checkIdleConnTask = new CheckIdleConnTask();
        //Периодичность запуска потока очистки
        long checkIdleConnTaskTimeOut = 5000;
        timer.scheduleAtFixedRate(checkIdleConnTask, 0, checkIdleConnTaskTimeOut);


    }

    private void printCurrentConn()
    {
        System.out.println("\nТекущее состояние пула");
        System.out.printf("Время %1$tT %1$td.%1$tm.%1$tY\n" +
                "В пуле использованых %2$d соединений\n" +
                "В пуле бездействующих %3$d соединений\n\n", new Date(), state.usedConnections.size(), state.availableConnections.size());

    }


    private Connection createConnection() throws SQLException {

        return  DriverManager
                .getConnection(URL, USER, PASSWORD);

    }




    //Получаем соединение из пула
    //Принцип: создаем отдельную задачу, которая будет пытаться получить соединение,
    // чтобы не задерживать текущий поток. Если задача по получению соединения не успела в срок, то выдаём исключение
    public ConnectionWrapper getConnection() throws Exception {



        long millisLimit_time = System.currentTimeMillis() + millisToWaitFreeConnection;//мс До какого времени будем пытаться получить соединение из пула



        CheckForAvailableConn checkForAvailableConn = new CheckForAvailableConn(this);//создаем экземпляр класса, который содержит дочерний поток, который будет пытаться получить соединение
        checkForAvailableConn.StartCheck();

        while (checkForAvailableConn.getConn() == null && millisLimit_time >= System.currentTimeMillis()) {//Ждем пока дочерний поток не получит соединение или истечет время ожидания
            Thread.sleep(millisTimeToWaitBeforeNextTry);//немного ожидаем перед новой проверкой на наличие соединения
        }

            if (checkForAvailableConn.getConn() != null) {//если задача по получению соединения смогла получить соеденине вовремя, то отдаем его
                System.out.printf("Поток %s получил соединение\n", Thread.currentThread().getName());

                return new ConnectionWrapper(checkForAvailableConn.getConn()); //запаковываем в защищенное соединение
            }
            checkForAvailableConn.StopCheck();//останавливаем поток, который пытался получить открытое соединения, если он не остановлен

            System.out.printf("Не дождались свободного соединения в установленное время выдаем исключение для потока %s\n", Thread.currentThread().getName());
            printCurrentConn();

            throw new Exception();//если не дождались свободного соединения в установленное время выдаем исключение

    }




    public  void releaseConnection(Connection con) throws SQLException//Если нам отдают соединение то складываем его в пул бездействующих соединений
    {

        //Если нам дали запакованное защищённое соединение, то распаковываем его
        if (con.getClass().getSimpleName().equals("ConnectionWrapper"))
            con = ((ConnectionWrapper) con).getRealConnection();


        synchronized (state) {
            /*System.out.printf("Поток %s ждет отдачи соединения, занял state и заснул\n", Thread.currentThread().getName());
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            if (con != null) {
                state.usedConnections.remove(con);
                if (maxIdleConnection > state.availableConnections.size()) {
                    state.availableConnections.add(new availableConn(con));
                      System.out.printf("Соединение отдано в пул бездействующих соединений от потока %s\n", Thread.currentThread().getName());
                } else {
                    System.out.println("Нет места в пуле бездействующих соединений, закрываем соединение");
                    try {
                        con.close();
                    }
                    catch (SQLException e){e.printStackTrace();}

                }
                if (con.isClosed()) System.out.printf("Соединение от потока %s закрыто\n", Thread.currentThread().getName());
                printCurrentConn();

            }
            else {System.out.println("Передано пустое соединение!");}
        }

    }



}
/*В целях тестирования пула соединений мы будем создавать несколько потоков, которые будут пытаться получать соединение из
 * пула соединений, делать запрос, удерживать соединение в течении рандомного времени и отдавать обратно.
 *  А затем спустя некоторое время снова создаем несколько потоков с такой же целью*/
public class Main {

    public static void main(String[] args) {
        System.out.println("Main thread запущен...");
        //Диапазон времени которое будет ожидать поток, перед тем как отдать соединение
        final int minThreadSleepTime = 10000; // мс
        final int maxThreadSleepTime = 30000; // мс

        int threadCount = 500;//Количество потоков для тестирования

        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        String url = "jdbc:sqlserver://localhost:1433;databaseName=testDB";
        String user = "test";
        String password = "12345";

        int maxActiveConnection = 50;//макс открытых соединений
        int maxIdleConnection = 20; //макс бездействующих соединений
        long millisToWaitFreeConnection = 20000;//мс ожидания на выдачу соединения из пула до вывода исключения
        long millisToFreeIdleConn = 10000;//мс до закрытия и удаления бездействующего соединения

        try {

            ConnectionPool connPool = new ConnectionPool(driver, url, user, password, maxActiveConnection, maxIdleConnection, millisToWaitFreeConnection,millisToFreeIdleConn);



            //лямбда функция описывающая поведение потоков, которые мы будем создавать
            Runnable r = ()->{
                System.out.printf("%s запущен... \n", Thread.currentThread().getName());

                try {
                    ConnectionWrapper connection = //DriverManager.getConnection(url, user, password);
                        connPool.getConnection() ;
                    if (connection!=null){
                    System.out.printf("Соединение с СУБД выполнено. Поток %s \n",Thread.currentThread().getName());
                    // делаем тестовый запрос к базе
                        Statement statement = connection.createStatement();
                        String sql = "Select Id from TestTable";
                        ResultSet rs = statement.executeQuery(sql);
                        /*if (rs!=null)                       // Fetch on the ResultSet
                        // Move the cursor to the next record.
                        while (rs.next()) {
                           *//* int Id = rs.getInt(1);
                            System.out.println("--------------------");
                            System.out.println("Id:" + Id);*//*
                        }*/

                        if (rs==null) {System.out.println("Результат пуст!"); }

                    connection.close();
                    Thread.sleep( rnd(minThreadSleepTime, maxThreadSleepTime)); //перед тем как отдать соединение поток останавливается

                   connPool.releaseConnection(connection); // отключение от БД
                     System.out.printf("Отключение от СУБД выполнено. Поток %s \n", Thread.currentThread().getName());
                    }
                }
                catch(InterruptedException e){
                    System.out.println("Поток был прерван...");
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                System.out.printf("%s завершен... \n", Thread.currentThread().getName());



            };

            //Создаем потоки которые будут делать соединение к базе, ожидать рандомное время и отдовать соединие
            for (int i=0;i<threadCount;i++)
            {
                new Thread(r,"Поток_"+i).start();
            }

            Thread.sleep(5000);//Ждем немного и снова создаем кучу потоков, которые хотя полуить соединение

            for (int i=0;i<threadCount;i++)
            {
                new Thread(r,"Новый поток_"+i).start();
            }



        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Ошибка SQL !");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static int rnd(int min, int max)
    {
        max -= min;
        return (int) (Math.random() * ++max) + min;
    }

}
