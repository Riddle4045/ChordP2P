import akka.actor.ActorSystem;
import akka.actor.ActorSelection;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Actor;
import akka.actor.ActorIdentity;
import akka.actor.Identify;
import akka.actor.Props;
import akka.pattern.AskableActorSelection;
import akka.util.Timeout;
import java.math.BigInteger;
import akka.actor.ActorRef;
import com.sun.org.apache.xpath.internal.operations.Bool;
import scala.collection.immutable.HashMap;




class Master extends Actor {
          //get the counter and shut Down when equal to numNodes.
           //all the chord peers will be child of this ?? how do we do that?
           //will terminate the code.
  var counter = 0;
  def receive = {
    case n : Int => println("Node complemeted work");
          counter = counter +1; 
          if(counter == 1){
                    context.stop(self)
          }
  }
}

class Node(numNodes : Int , m:Int, hashOfFistNode : String) extends Actor {
          //TODO : implement a data strcuture to hold finger table for the guy.
          //TODO : impleemnt three functions find_sucessor find_predecessor and closest_preceding_finger.
  
  var finger= Array.ofDim[Int](m, 3);
  
  for(i<-0 to m){
    
  }
  
  // private values of a node 
  var fingerTable = 0;
  var sucessor = null;
  var  predecessor = null;
  var nodeId 
  
  def receive = {
    case s : String => println("got a string");
    case "firstNode"  => initFingerTable();
    case "New Node" => join(hashOfFirstNode);
  }
  

  def join(n : Int ) = {
       if(isNode(n)){
               init_finger_table(n);
               ipdate_tables(n);
       }else{
             for( i <- 1 to m){
                   finger[i][0] = nodeId;
             }
             predecessor = nodeId;
       }
       
  }
  
  def init_finger_table(n : Int) = {
      
  }
  
  def update_others() = {
      
  }
  def find_successor(id:Int) : Int = {
    var temp = find_predecessor(id);
   
    return temp.successor;
   
  }
  
  
  
  //this function will be called on init if a node.
  //so in the beiging all the nodes will be sucessor and predecessor of themseleves.
  //sets the value of the sucessor and predecessor private variable of the node.
 def sucessor(NodeId: Int) : Int = {
   return 0
 
 }
 
 def find_predecessor(id : Int ) : Int = {
       var temp = nodeId;
       var temp_successor = temp.successor;
      while((id < temp) || (id>temp.successor)){
         temp = closest_preceeding_finger(id);
       }
      return temp;
 }
 
 def closest_preceeding_finger(id : Int) : Int = {
   for(i<-m to 1){
     if((finger[i][1]>nodeId) && (finger[i][1] < id)){
       return finger[i][1];
     }
     return nodeId;
   }
   
 }
 
 //this function sends numRequests requests and upon completion stops sending requests
 //note that it doesnt shut down.
 def initaite_peer()={
       
 }
 
 def checkExistence(id : Int) = {

    ActorSystem sys = ActorSystem.apply("test");
    sys.actorOf(Props.create(TestActor.class), "mytest");
    ActorSelection sel = sys.actorSelection("/user/mytest");

    Timeout t = new Timeout(5, TimeUnit.SECONDS);
    AskableActorSelection asker = new AskableActorSelection(sel);
    Future<Object> fut = asker.ask(new Identify(1), t);
    ActorIdentity ident = (ActorIdentity)Await.result(fut, t.duration());
    ActorRef ref = ident.getRef();
    System.out.println(ref == null);
   
}
 


}
/**
 * @author jdas
 */
object Project3 {
  
 
  def main(args:Array[String]){
    
    //TODO : Extract the arguments as numNodes and numRequests
    //TODO : figure out value of "m" from the numNodes.
    //TODO : Initialize chord network through JOIN or iteratively.
    //TODO : implement find+predecessor and successor functions .
    //TODO : send messages from each peer.
    //TODO : stop the program when all the peers send numRequests messages
//    
    if(args.length < 2){
              System.out.println("Not enough arguments");
              
    }
      val numNodes = Integer.parseInt(args(0));
      val numRequests = Integer.parseInt(args(1));
    
      
   var hashOfFirstNode;
   // save the id of first node we create.
   //second node onwards join.
    //m can be choosen such that 2^m -1 is nearly 1000 times the number of nodes?
    //00000000111010101010101010101010101010..........
    //m- bits -->   000000 to 1111111   -->> decimal number 0 to dec(1111111) 
    var m=0;   //this is the m-bits from paper.
    var temp = 0;
    while ( temp < (numNodes*10000).toInt){
          temp = Math.pow(2,m).toInt;
          m = m+1;
          
    }
    val system = ActorSystem("Master");
    //System.out.println(m);
    var actor : ActorRef = null;
     for(i<-0 to numNodes-1){
       
       val name = createIdentifier(m,"node"+i);
       val system = ActorSystem("Master");
       actor = system.actorOf(Props(new Master), name = name);
     }
    
     
     //System.out.println(10.toBinaryString);
    /* val binaryString = String.format("%"+Integer.toString(m)+"s",Integer.toBinaryString(m));
     System.out.println(binaryString);*/
    
     
  }
   
 def createIdentifier(m:Int,nodeName:String) : String = {
  
        val str = SHA(nodeName.toString);
        var arr = str.toString().getBytes();
   var tem = "";
     for( i <- 0 to m){
           if(isSet(arr,i)){
             tem = tem+"1";
           }
           else{
             tem = tem+"0";
           }
     }
     
    var  cryptedName = Integer.parseInt(tem, 2);
    
    return cryptedName.toString;
           
 }
   
 def  isSet(arr : Array[Byte],bit : Int) : Boolean =  {
    var index = bit / 8;  // Get the index of the array for the byte with this bit
    var bitPosition = bit % 8;  // Position of this bit in a byte

    return (arr(index) >> bitPosition & 1) == 1;
}
  
  
 def SHA(s: String): String = {
    // Besides "MD5", "SHA-256", and other hashes are available
    val m = java.security.MessageDigest.getInstance("SHA-1").digest(s.getBytes("UTF-8"))
    m.map("%02x".format(_)).mkString
  }
}
  
  
  
  
  
  
   
  

  
  
   
  
