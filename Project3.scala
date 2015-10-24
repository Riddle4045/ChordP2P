import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import com.sun.org.apache.xpath.internal.operations.Bool
import scala.collection.immutable.HashMap
import java.lang.Long
import scala.concurrent.duration.Duration
import scala.actors.threadpool.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorSelection
import java.util.Timer
import scala.util.Success
import scala.util.Failure
import java.math.BigInteger
import scala.util.control.Breaks
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await


trait Message
case object getSucessor extends Message
case class Find_Sucessor(n : Int) extends Message
case class Find_Predecessor(n : Int)extends Message
case class Set_Predecessor(n : Int) extends Message
case class Update_Finger_Table(n : Int,i : Int) extends Message


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

class Node(numNodes : Int , m:Int, hashOfFistNode : Int, system : ActorSystem,nodeid : Int) extends Actor {
          //TODO : implement a data strcuture to hold finger table for the guy.
          //TODO : impleemnt three functions find_sucessor find_predecessor and closest_preceding_finger.
  
  var finger= Array.ofDim[Int](m, 3);
  
  for(i<-0 to m){
    
  }
  
  // private values of a node 
  var fingerTable = 0;
  var sucessor = 0;
  var  predecessor = 0;
  var nodeId =0;
  
  def receive = {
    case "firstNode"  => nodeId = nodeid
      initFingerTable();
    case "New Node" => join(hashOfFistNode);
    case getSucessor => sender ! sucessor
    case Find_Sucessor(id) => sender ! find_successor(id)
    case Find_Predecessor(id) => sender! find_predecessor(id)
    case Set_Predecessor(id) => predecessor = id;
    case Update_Finger_Table(n,i) => update_finger_table(n, i)
  }
  
  def initFingerTable() ={
        for ( i <- 1 to m){
              finger(i)(1) = nodeId;
        }
        predecessor  = nodeId;
  }

  def join(n : Int ) = {
       if(isExsist(n)){
               init_finger_table(n);
               update_others();
       }else{
             for( i <- 1 to m){
                   finger(i)(0)= nodeId;
             }
             predecessor = nodeId;
       }
       
  }
  
  def init_finger_table(n : Int) = {
       var n_actor = getActorRef(n);
       implicit val timeout = Timeout(1 seconds)
       var n_future =  n_actor ? Find_Sucessor(finger(1)(0)); //finger(1)(0) we should replace with nodeId + 1;
       finger(1)(1) = Await.result(n_future,timeout.duration).asInstanceOf[Int]
       sucessor = finger(1)(1);
       var m_actor = getActorRef(finger(1)(1));
       var m_future = m_actor ? Find_Predecessor(finger(1)(1));
       predecessor = Await.result(m_future,timeout.duration).asInstanceOf[Int]
       m_actor ! Set_Predecessor(nodeId);
       
       
       //now update the finger table.
       for(i <- 1 to m-1){
         if((finger(i+1)(0) >= n)&&(finger(i+1)(0) <  finger(i)(1))){
           finger(i+1)(1) = finger(i)(1);
         }
         else{
                  var o_actor = getActorRef(n);
                  var o_future = o_actor ? Find_Sucessor(finger(i+1)(0));
                  finger(i+1)(1) =  Await.result(o_future,timeout.duration).asInstanceOf[Int]
         }
       }
  }
  
  def update_others() = {

      for(i<- 1 to m){
        val index= nodeId - Math.pow(2,i-1).toInt;
       var n_actor = getActorRef(index);
       implicit val timeout = Timeout(1 seconds)
       var n_future =  n_actor ? Find_Predecessor(finger(1)(0));
       var p = Await.result(n_future,timeout.duration).asInstanceOf[Int]    
       var p_actor = getActorRef(p);
       p_actor ! Update_Finger_Table(nodeId,i);
        
      }
  }
  
  def update_finger_table(s : Int , i : Int) = {
      if(s>= nodeId && s<finger(i)(1)){
        finger(i)(1) = s;
         var p = predecessor;
               var p_actor = getActorRef(p);
       p_actor ! Update_Finger_Table(s,i);
          
      }
      
  }
 
  def find_successor(id:Int) : Int = {
    var temp = find_predecessor(id);
    var  temp_actor : ActorRef = null;
    var temp_successor : Int = 0;
   if (isExsist(temp)) {
           temp_actor = getActorRef(temp);
           implicit val timeout = Timeout(1 seconds)
           var future = temp_actor ? getSucessor // enabled by the “ask” import
           temp_successor = Await.result(future, timeout.duration).asInstanceOf[Int];
           
   }
    return temp_successor;
   
  }
  

 def find_predecessor(id : Int ) : Int = {
       var temp = nodeId;
        var  temp_actor : ActorRef = null;
        var temp_successor : Int = 0;
       if (isExsist(temp)) {
               temp_actor = getActorRef(temp);
               implicit val timeout = Timeout(5 seconds)
               var future = temp_actor ? getSucessor // enabled by the “ask” import
               temp_successor = Await.result(future, timeout.duration).asInstanceOf[Int];
               
       }
      while((id < temp) || (id > temp_successor)){
         temp = closest_preceeding_finger(id,m);
       }
      return temp;
 }
 
 
 def closest_preceeding_finger(id : Int, m : Int) : Int = {
         var res = nodeId;
         var loop = new Breaks;
         loop.breakable  {
          for(  i <- m to 1 by -1)   {
                        if( (finger(i)(1) > id) && (finger(i)(1) < nodeId) ){
                                  res = finger(i)(1); loop.break;
                        }
          }
         } 
   return res;
 }
 
 //this function sends numRequests requests and upon completion stops sending requests
 //note that it doesnt shut down.
 def initaite_peer()={
       //initialize the table 
 }
 
 //get the reference of the actor from the id
 def getActorRef(temp : Int) : ActorRef ={
             var isFound : ActorRef= null;
                      implicit val timeout = 1000;
                       var name = "akka://Master/user/" + temp.toString();
                    system.actorSelection(name).resolveOne()(timeout).onComplete {
                      case Success(actor) => 
                                 isFound = actor;
                      case Failure(ex) =>
                              println("actor not found");
                           
                        }
   return isFound;
 }
 
 //check if the actor with this id exsists.
 def isExsist(id : Int) : Boolean = {
    var isFound = false;
                      implicit val timeout = 1000;
                       var name = "akka://Master/user/" + id.toString();
                    system.actorSelection(name).resolveOne()(timeout).onComplete {
                      case Success(actor) => 
                                 isFound = true;
                      case Failure(ex) =>
                              println("actor not found");
                              isFound = false;
                        }
   return isFound;
}
 

}
/**
 * @author jdas
 */
object Project3 {
  
 
  def main(args:Array[String]){
    

    if(args.length < 2){
              System.out.println("Not enough arguments");
              
    }
      val numNodes = Integer.parseInt(args(0));
      val numRequests = Integer.parseInt(args(1));
    
      
   var hashOfFirstNode = 0;
    var m=0;   //this is the m-bits from paper.
    var temp = 0;
    while ( temp < (numNodes*10000).toInt){
          temp = Math.pow(2,m).toInt;
          m = m+1;
          
    }
    val system = ActorSystem("Master");
    var actor : ActorRef = null;
    var name = createIdentifier(m,"node"+0);
    hashOfFirstNode = name.toInt;
    actor =  system.actorOf(Props(new Node(numNodes,m,0,system,name.toInt)), name = name);
    actor ! "firstNode";
     for(i<-1 to numNodes-1){
       
        name = createIdentifier(m,"node"+i);
       actor = system.actorOf(Props(new Node(numNodes,m,hashOfFirstNode,system,name.toInt)), name = name);
       actor ! "New Node"
     }
    
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
  
  
  
  
  
  
   
  

  
  
   
  
