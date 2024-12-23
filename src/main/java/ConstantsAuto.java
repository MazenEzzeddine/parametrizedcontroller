import java.sql.SQLOutput;

class ConstantsAuto {

    public static int nbpartitions;
     public static long decisionInterval;
     public static int initialreplicas;

     public static double wsla;
     public static double rebtime;

     public static String[] partitionsarrivals;
     public static String[] partitionsLag;

    public  static String processingQuery;


    public static String  topic;
    public static String consumergroup;
     public static String bootstrapservers;


     public static float fup;
     public static float fdown;
     public static Long WarmupTime;





     public static void init() {
         nbpartitions = Integer.parseInt( System.getenv("NB_Partitions"));
         decisionInterval = Long.parseLong(System.getenv("DECISION_INTERVAL"));
         initialreplicas = Integer.parseInt(System.getenv("INITIAL_REPLICAS"));
         wsla = Double.parseDouble(System.getenv("WSLA"));
         rebtime = Double.parseDouble(System.getenv("REB_TIME"));
        topic =  System.getenv("TOPIC");
        consumergroup = System.getenv("CONSUMER_GROUP");
        bootstrapservers  = System.getenv("BOOTSTRAP_SERVERS");
         fup= Float.parseFloat(System.getenv("FUP"));
         fdown= Float.parseFloat(System.getenv("FDOWN"));
         WarmupTime = Long.parseLong(System.getenv("WARMUP_TIME"));


         partitionsarrivals = new String[nbpartitions];

         partitionsLag = new String[nbpartitions];

     }


    public static  void initPrometheusQueries() {
         int rate;

         if( decisionInterval <= 20000)
         {
             rate = 20;
         }
         else {
             rate = (int) (decisionInterval/1000);
         }


         for (int i = 0; i < nbpartitions; i++) {
             partitionsarrivals[i] = "http://prometheus-operated:9090/api/v1/query?" +
                     "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%22"
                     +i+"%22,namespace=%22default%22%7D%5B"+ rate +"s%5D))";
         }


         for (int i = 0; i < nbpartitions; i++) {
             partitionsLag[i] = "http://prometheus-operated:9090/api/v1/query?query=" +
                     "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%22"+
                     i+"%22,namespace=%22default%22%7D";
         }


      processingQuery   = "http://prometheus-operated:9090/api/v1/query?query=" +
                 "1000/(avg(rate(events_latency_sum["+ rate+ "s])/rate(events_latency_count[" + rate +"s])))";

     }



     public static void printParameters() {
         //TODO

         System.out.println("NB_Partitions:" + nbpartitions);
         System.out.println("DECISION_INTERVAL " + decisionInterval);
         System.out.println("INITIAL_REPLICAS " + initialreplicas);
         System.out.println("WSLA " + wsla);
         System.out.println("REB_TIME " + rebtime);
         System.out.println("CONSUMER_GROUP " + consumergroup);
         System.out.println("FUP " + fup);
         System.out.println("FDOWN " + fdown);
         System.out.println("WARMUP_TIME " + WarmupTime);




     }
}
