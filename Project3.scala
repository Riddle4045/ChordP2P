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
case class Find_Sucessor(n : Int) extends Message
case class Find_Predecessor(n : Int)extends Message
case class Set_Predecessor(n : Int) extends Message
case class Update_Finger_Table(n : Int,i : Int) extends Message


class Master extends Actor {

  var counter = 0;
  def receive = {
    case n : Int => println("Node complemeted work");
          counter = counter +1; 
          if(counter == 1){
                    context.stop(self)
          }
  }
}

class Node(numNodes : Int , m:Int, hashOfFistNode : Int, system : ActorSystem,nodeid : Int, temp : collection.mutable.Map[Int,ActorRef]) extends Actor {
  var finger= Array.ofDim[Int](m, 2);

  
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
    //printFingerTable();
    case "New Node" => nodeId = nodeid
      getFirstStartForNode()
      join(hashOfFistNode)
    //  printFingerTable();
    case Find_Sucessor(id) => println("Message recieved Find_Sucessor with an id  :" + id)
      sender ! find_successor(id)
    case Find_Predecessor(id) => sender! find_predecessor(id)
    case Set_Predecessor(id) => println(nodeId + "  has the predecessor as :" + id);
        predecessor = id;
    case Update_Finger_Table(n,i) => update_finger_table(n, i)
    case "fetchSucessor" => println("recieved getSucessor message")
        sender ! sucessor;
  }
  
  def printFingerTable() = {
    for(i<- 0 to m-1)
    {
      for(j<- 0 to 1){
        System.out.print(finger(i)(j)+" ");
      }
      println;
    }
   
    
  }
  
  def getFirstStartForNode() = {
    
    for ( i <- 0 to m-1){
              finger(i)(0) = (nodeId + Math.pow(2,i).toInt) % (Math.pow(2,m).toInt);
              finger(i)(1) = 0;         
     
  }
  }
  
  def initFingerTable() ={
        for ( i <- 0 to m-1){
              finger(i)(0) = (nodeId + Math.pow(2,i).toInt) % (Math.pow(2,m).toInt);
              finger(i)(1) = nodeId;
             
              
        }
        predecessor  = nodeId;
  }

  def join(n : Int ) = {
      var isFound = isExsist(n);
   //   println("Inside join, the node " + n + "exsists :" + isFound);
       if(isExsist(n)){
               init_finger_table(n);
               update_others();
         //      printFingerTable()
       }else{
             for( i <- 0 to m-1){
                   finger(i)(0)= nodeId;
             }
             predecessor = nodeId;
       }
       
  }
  
  def init_finger_table(n : Int) = {
     //  println("populating first entry of finger table of new node with id " + nodeId);
       var n_actor : ActorRef = getActorRef(n);
       implicit val timeout = Timeout(1 seconds)
       var n_future =  n_actor ? Find_Sucessor(finger(0)(0)); //finger(1)(0) we should replace with nodeId + 1;// (nodeId + 2^(i-1)) % 2^m
       finger(0)(1) = Await.result(n_future,timeout.duration).asInstanceOf[Int]
     //  println(nodeId + " has finger table's first entry as : " + finger(0)(0) + "  " + finger(0)(1))
       sucessor = finger(0)(1);
       var m_actor = getActorRef(finger(0)(1));
       var m_future = m_actor ? Find_Predecessor(finger(0)(1));
       predecessor = Await.result(m_future,timeout.duration).asInstanceOf[Int]
      // println(nodeId +  "  now has the predecessor as :" + predecessor);
       m_actor ! Set_Predecessor(nodeId);
       

       for(i <- 0 to m-2){
          var id  = nodeId + Math.pow(2,i+1).toInt
         var finger_i = nodeId + Math.pow(2,i).toInt;
         var actor_finger_i_node = finger(i)(1);
         if(isWithinRangeFirstOpen(nodeId, actor_finger_i_node, id)){
         //    println("finge table of node " + nodeId + " row :" + i+1 + " is between " + nodeId + " and" + actor_finger_i_node );
             finger(i+1)(1) = finger(i)(1);
           //  println(nodeId + " has finger table's  entry as : " + finger(i+1)(0) + "  " + finger(i+1)(1))
         }
         else{
               //  println(nodeId + "'s finger tables row " + (i+1)+ " entry is not in range"); 
                  var o_actor = getActorRef(n)
                  var o_future = o_actor ? Find_Sucessor(finger(i+1)(0));
                  finger(i+1)(1) =  Await.result(o_future,timeout.duration).asInstanceOf[Int]
            //      println(nodeId + " has finger table's  entry as : " + finger(i+1)(0) + "  " + finger(i+1)(1)) 
                  
         }
       }
  
  }
  
  def update_others() = {
  //  println( nodeId  +  "  update others");
      for(i<- 0 to m-1){
        println("HERE THE I STARTS _____ " + i);
       var index= nodeId - Math.pow(2,i).toInt;
       if(index < 0) index = Math.pow(2,m).toInt + index;
    //   println("going back in update others of " + nodeId + " index is :" + index);
       var p = find_predecessor(index);
    //   println("after finding predecessor of index  :" + index + " p is : " + p);
      // var p_actor = getActorRef(p);
       var p_actor = temp(p.toInt)
          p_actor ! Update_Finger_Table(nodeId,i);
      }
  }
  
  def update_finger_table(s : Int , i : Int) = {
    // println("inserting " + s + "between" + nodeId + " and " + "finger(" +i +")(1)");
    //  println("update finger table of node " + nodeId + " with :" + s);
   // if( i == 2) println(" I is : " + i + " node is " + nodeId + " and finger(i)(0) is " + finger(i)(0)+ " finger(i)(1)" + finger(i)(1));
    if( i == 1)  println(" I is : " + i + " node is " + nodeId + " and finger(i)(0) is " + finger(i)(0) + " finger(i)(1)" + finger(i)(1));
    var isRange = isWithinRangeFirstClosed(nodeId, finger(i)(1),s);
    println(isRange)
      if(isRange){
        println(nodeId +  " updated  finger(" +i+ ")" + "(1)" + " to " + s)
         var p = predecessor;
        println("first node preceding " + nodeId  + " is " + p);
      //  var p_actor = getActorRef(p);
       var p_actor = temp(p.toInt);
       p_actor ! Update_Finger_Table(s,i);
        
      }else{

      }
  }
 
  def find_successor(id:Int) : Int = {
   // println("finding the sucessor if id :" + id);
    var temp = find_predecessor(id);
    var  temp_actor : ActorRef = null;
    var temp_successor : Int = 0;
   // println("found the predecessor and its " + temp);
   if (isExsist(temp)) {
          if(temp != nodeId){
           temp_actor = getActorRef(temp);
           implicit val timeout = Timeout(10 seconds)
           println("do or die")
           var future = temp_actor ? "fetchSucessor" // enabled by the “ask” import
           temp_successor = Await.result(future, timeout.duration).asInstanceOf[Int];
          }else{
              temp_successor = finger(0)(1);
          }
   }
    return temp_successor;
   
  }
  

 def find_predecessor(id : Int ) : Int = {
  // println(nodeId + " is finding predecessor of  " + id );
       var temp = nodeId;
        var  temp_actor : ActorRef = null;
        var temp_successor : Int = 0;
     /**  if (isExsist(temp)) {
             println("isExists succeded")
             if(temp != nodeId){
               temp_actor = getActorRef(temp);
               implicit val timeout = Timeout(5 seconds)
               var future = temp_actor ? "fetchSucessor" // enabled by the “ask” import
               temp_successor = Await.result(future, timeout.duration).asInstanceOf[Int];
               println("got temp_sucessor as : " + temp_successor)
             }else {
               temp_successor = finger(0)(1);
             }
              
       }**/
        temp_successor = finger(0)(1);
  //     println("temp and temp_sucessor are  : " + temp + "and" + temp_successor + "and the id is :" + id)
       if(temp!=temp_successor){
               while(!isWithinRange(temp,temp_successor,id)){
                   //      println("not in range")
                           temp = closest_preceeding_finger(id, m); 
               }
            //   println("returining temp because not in range")
               return temp;
       }else{
         //     println("returning temp");
                return temp;
       }
 }
 //check is id belongs to (temp,temp_sucessor].
 def isWithinRange(temp :Int , temp_successor : Int, id : Int ) : Boolean= {
     var i  =1;
       if(id == temp_successor) return true;
       while((temp+i) % Math.pow(2,m).toInt != temp_successor && i < Math.pow(2,m)){
           if((temp+i) % Math.pow(2,m) == id) return true;
           else  i = i +1;
       }
     return false;
 }
  //check is id belongs to (temp,temp_sucessor).
 def isWithinRangeFirstOpen(temp :Int , temp_successor : Int, id : Int ) : Boolean= {
     var i  =0;
       while((temp+i) % Math.pow(2,m).toInt != temp_successor && i < Math.pow(2,m)){
           if((temp+i) % Math.pow(2,m) == id) return true;
           else  i = i +1;
       }
     return false;
 }
 
 //check if id belongs to (temp,temp_sucessor)
  def isWithinRangeBothOpen(temp :Int , temp_successor : Int, id : Int ) : Boolean= {
     var i  =1;
       while((temp+i) % Math.pow(2,m).toInt != temp_successor && i < Math.pow(2,m)){
           if((temp+i) % Math.pow(2,m) == id) return true;
           else  i = i +1;
       }
     return false;
 }
  //check if id belongs to [temp,temp_sucessor)
  def isWithinRangeFirstClosed(temp :Int , temp_successor : Int, id : Int ) : Boolean= {
         var i  =1;
         var toReturn = false;
       //  if(id == temp) return true;
         var loop = new Breaks;
         loop.breakable  {
                   while((temp+i) % Math.pow(2,m).toInt != temp_successor && i < Math.pow(2,m)){
                       if((temp+i) % Math.pow(2,m).toInt == id) {
                         { var r = temp + i ;println((temp+i) %Math.pow(2,m).toInt + " id" + id);}
                         toReturn = true;
                         loop.break;;
                       }
                       else  i = i +1;
                   }
         }
     return toReturn;
 }
 
 def closest_preceeding_finger(id : Int, m : Int) : Int = {
         var res = nodeId;
         var loop = new Breaks;
         loop.breakable  {
          for(  i <- m-1 to 0 by -1)   {
                        if( isWithinRangeBothOpen(nodeId,id,finger(i)(1)) ){
                                  res = finger(i)(1); loop.break;
                        }
          }
         } 
   return res;
 }
  
 //get the reference of the actor from the id
 def getActorRef(temp : Int) : ActorRef ={
    //   println(nodeId + "  " + temp);
       if(temp == nodeId) return self;
             implicit val timeout = Timeout(1 second);
             var name = "akka://Master/user/" + temp.toString();
                /**
                   system.actorSelection(name).resolveOne()(timeout).onComplete {
                      case Success(actor) => 
                                 isFound = actor;
                      case Failure(ex) =>
                              println("actor not found");
                           
                        }**/
                   var future =   system.actorSelection(name).resolveOne()(timeout);
                   var actor : ActorRef = Await.result(future,timeout.duration).asInstanceOf[ActorRef];      
      return actor;
 }
 
 //check if the actor with this id exsists.
 def isExsist(id : Int) : Boolean = {
   if(id== nodeId ) return true;
   var isFound = false;
                      
                      implicit val timeout = Timeout(1 second);
                       var name = "akka://Master/user/" + id.toString();
                  //      println("node: " + nodeId + "checking for actor "  + name);
               /**     system.actorSelection(name).resolveOne()(timeout).onComplete {
                      case Success(actor) => println("here")
                                 isFound = true; 
                                 println("value of is found : " + isFound);
                      case Failure(ex) =>
                              println("actor not found");
                              isFound = false; 
                        }
                        *
                        */
                   var future =   system.actorSelection(name).resolveOne()(timeout);
                   var actor = Await.result(future,timeout.duration).asInstanceOf[ActorRef];
                   if(actor != null) isFound = true;
            //        println("isFound after checking for id is: " + isFound )
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
    while ( temp < (numNodes*10).toInt){
          temp = Math.pow(2,m).toInt;
          m = m+1;
          
    }
        val system = ActorSystem("Master");
    var actor_chagan =  system.actorOf(Props(new Master), name = "chagan");
    var mymap = collection.mutable.Map(-1 -> actor_chagan);
    m = 3;

    var actor : ActorRef = null;
    var name = createIdentifier(m,"node"+0);
    hashOfFirstNode = name.toInt;
    actor =  system.actorOf(Props(new Node(numNodes,m,0,system,name.toInt,mymap)), name = name);
    mymap += name.toInt -> actor;
    println("first node created with id :" + name.toInt);
    actor ! "firstNode";
     for(i<-1 to numNodes-1){
       
        name = createIdentifier(m,"node"+i);
        actor = system.actorOf(Props(new Node(numNodes,m,hashOfFirstNode,system,name.toInt,mymap)), name = name);
        mymap  += name.toInt -> actor;
       println("new node created with id : " + name.toInt);
       actor ! "New Node"
     }
    
  }
   
 def createIdentifier(m:Int,nodeName:String) : String = {
  
        val str = SHA(nodeName.toString);
        var arr = str.toString().getBytes();
        var tem = "";
        for( i <- 0 to m-1){
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
