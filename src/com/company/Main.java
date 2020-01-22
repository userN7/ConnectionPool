package com.company;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;



class ConnectionPool {
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
    //с помощью этой задачи будем проверять пришел ли срок удаления бездействующего соединения
    private class CheckIdleConnTask extends TimerTask {

        @Override
        public synchronized void run() {
            System.out.printf("\nЗадача очистки бездействующих соединений запущена\n");


            for (int i = 0; i < availableConnections.size(); i++) {
                try {
                    //Если соединение просрочено то закрываем и удаляем из пула
                    if (availableConnections.get(i).GetLifeTimeStamp() < System.currentTimeMillis()) {
                        if (availableConnections.get(i).GetConnection() != null)
                            availableConnections.get(i).GetConnection().close();
                        availableConnections.remove(availableConnections.get(i));
                        System.out.println("Очистили бездействующее соединение");
                        printCurrentConn();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }



            System.out.printf("\nЗадача очистки бездействующих отработала\n\n");
            completeTask();

        }

        private void completeTask() {
            try {
                //Ждем пока задача завершится
                //5 сек
                Thread.sleep(5000);
            } catch (InterruptedException e) {e.printStackTrace();}
        }
    }
    private List<availableConn> availableConnections = new ArrayList<>();
    private List<Connection>usedConnections = new ArrayList<>();
    private int maxActiveConnection;
    private int maxIdleConnection;
    private String URL;
    private String USER;
    private String PASSWORD;
    private long millisToWaitFreeConnection;
    private long millisToFreeIdleConn;


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

        //первично заполняем пул
        int initConnCount = 5;
        System.out.println("Первичное заполнение пула");
        initConnCount=initConnCount<maxIdleConnection?initConnCount:maxIdleConnection;//Проверяем больше ли лимита на бездействующие соединения
        for (int count = 0; count < initConnCount; count++) {
            availableConnections.add( new availableConn( this.createConnection()));
        }
        printCurrentConn();
        // Запускаем задение на проверку бездействующих соединений
        Timer timer = new Timer(true);
        TimerTask checkIdleConnTask = new CheckIdleConnTask();
        timer.scheduleAtFixedRate(checkIdleConnTask, 0, this.millisToFreeIdleConn);


    }

    private void printCurrentConn()
    {
        System.out.printf("\nТекущее состояние пула:\n");
        System.out.printf("В пуле использованых %d соединений\n",usedConnections.size());
        System.out.printf("В пуле бездействующих %d соединений\n\n", availableConnections.size());
    }


    private  Connection createConnection() throws SQLException {

        return DriverManager
                .getConnection(URL, USER, PASSWORD);

    }




    //Получаем соединение из пула
    public synchronized Connection getConnection() throws Exception {

        long millisTimeToWaitBeforeNextTry = 100;//мс Сколько будем ждать перед новой получить соединение
        long millisLimit_time = System.currentTimeMillis() + millisToWaitFreeConnection;//мс До какого времени будем пытаться получить соединение из пула
        //Ждем пока появятся свободные соединения или истечет время ожидания
        //можно добавить задержку между попытками получить новое соединие
        do {
            //Смотрим не превышен лимит на открытые соединения
            if (maxActiveConnection > usedConnections.size()) {
                //если есть доступные соединения в пулы то выдаем
                if (!availableConnections.isEmpty()) {
                    Connection con = availableConnections.get(availableConnections.size() - 1).GetConnection();
                    availableConnections.remove(availableConnections.size() - 1);
                    usedConnections.add(con);
                    System.out.println("Выдано соединение из пула бездействующих соединений");
                    printCurrentConn();
                    return con;
                }
                //иначе создаем новое
                else {
                    Connection con = createConnection();
                    usedConnections.add(con);
                    System.out.println("Создано новое соединение для пула");
                    printCurrentConn();
                    return con;
                }
            }
            //если не можем выдать то ждем немного и снова пробуем
            else {
                Thread.sleep(millisTimeToWaitBeforeNextTry);
            }
        } while (millisLimit_time>System.currentTimeMillis());


        //если не дождались свободного соединения в установленное время выдаем исключение
        //return null;
        System.out.println("Не дождались свободного соединения в установленное время выдаем исключение");
        printCurrentConn();
        throw new Exception();
    }




    public synchronized void releaseConnection(Connection con) throws SQLException {
        //Если нам отдают соединение то складываем его в пул бездействующих соединений
        if (null != con) {
            usedConnections.remove(con);
            if (maxIdleConnection > availableConnections.size()) {
                availableConnections.add(new availableConn(con));
                System.out.println("Соединение отдано в пул бездействующих соединений");
            }
            else {
                System.out.println("Нет места в пуле бездействующих соединений, закрываем соединение");
                con.close();
            }

            printCurrentConn();

        }

    }



}
/*В целях тестирования пула соединений мы будем создавать несколько потоков, которые будут пытаться получать соединение из
 * пула соединений, удерживать его в течении рандомного времени и отдавать обратно*/
public class Main {

    public static void main(String[] args) {
        System.out.println("Main thread запущен...");
        //Диапазон времени которое будет ожидеть поток, перед тем как отдать соединение
        final int minThreadSleepTime = 10000; // 10 секунд
        final int maxThreadSleepTime = 75000; // 75 секунд

        int threadCount = 15;//Количество потоков для тестирования

        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        String url = "jdbc:sqlserver://localhost:1433;databaseName=testDB";
        String user = "test";
        String password = "12345";

        int maxActiveConnection = 10;//макс открытых соединений
        int maxIdleConnection = 7; //макс бездействующих соединений
        long millisToWaitFreeConnection = 3000;//мс ожидания на выдачу соединения из пула до вывода исключения
        long millisToFreeIdleConn = 5000;//мс до закрытия и удаления бездействующего соединения

        try {

            ConnectionPool connPool = new ConnectionPool(driver, url, user, password, maxActiveConnection, maxIdleConnection, millisToWaitFreeConnection,millisToFreeIdleConn);



            //лямбда функция описывающая поведение потоков, которые мы будем создовать
            Runnable r = ()->{
                System.out.printf("%s запущен... \n", Thread.currentThread().getName());

                try {
                    Connection connection = connPool.getConnection() ;//соединениесБД
                    System.out.printf("Соединение с СУБД выполнено. Поток %s \n",Thread.currentThread().getName());

                    //перед тем как отдать соединение поток останавливается от 10 до 75 сек
                    Thread.sleep( rnd(minThreadSleepTime, maxThreadSleepTime));
                    connPool.releaseConnection(connection);       // отключение от БД
                    System.out.printf("Отключение от СУБД выполнено. Поток %s \n",Thread.currentThread().getName());
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
