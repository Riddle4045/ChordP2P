


import akka.actor.Actor
import java.math.BigInteger

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

class Node extends Actor {
          //TODO : implement a data strcuture to hold finger table for the guy.
          //TODO : impleemnt three functions find_sucessor find_predecessor and closest_preceding_finger.
  
  var fingerTable = 0;
  def receive = {
    case s : String => println("got a string");
  }

  
  
  def join(n : Int ) = {
       
  }
  
  def init_finger_table(n : Int) = {
      
  }
  
  def update_others() = {
      
  }
  
 def find_sucessor(id: String) : Int = {
       return 0;
 }
 
 def find_predecessor(id : String ) : Int = {
       return 0;
 }
 
 def closest_preceeding_finger(id : String) : Int = {
   return 0;
 }
 
 //this function sends numRequests requests and upon completion stops sending requests
 //note that it doesnt shut down.
 def initaite_peer()={
       
 }
 


}
/**
 * @author jdas
 */
object Project3 {
  
 
  def main(args:Array[String]){
    
    //TODO : Extract the arguments as numNodes and numRequests
    //TODO : figure out value of "m" from the numNodes.
    //TODO : Initialize chord network throgh JOIN or iteratively.
    //TODO : implement find+predecessor and sucessor functions .
    //TODO : send messages from each peer.
    //TODO : stop the program when all the peers send numRequests messages
//    
    if(args.length < 2){
              System.out.println("Not enough arguments");
              
    }
      val numNodes = Integer.parseInt(args(0));
      val numRequests = Integer.parseInt(args(1));
    
    
    val k = SHA("Juthika");
    
    
    System.out.println(k);
    
    //m can be choosen such that 2^m -1 is nearly 1000 times the number of nodes?
    //00000000111010101010101010101010101010..........
    //m- bits -->   000000 to 1111111   -->> deimal number 0 to dec(1111111) 
    var m=0;   //this is the m-bits from paper.
    var temp = 0;
    while ( temp < (numNodes*10000).toInt){
          temp = Math.pow(2,m).toInt;
          m = m+1;
          
    }
    //System.out.println(m);
    var x = createIdentifier(k,m);
    var y = createIdentifier(k, m);
     //System.out.println(10.toBinaryString);
    /* val binaryString = String.format("%"+Integer.toString(m)+"s",Integer.toBinaryString(m));
     System.out.println(binaryString);*/
     System.out.println(x);
     System.out.println(y);
     
  }
   
 def createIdentifier(str:String,m:Int) : Int = {
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
     //System.out.println(tem);
     var x = Integer.parseInt(tem, 2);
     //System.out.println(x);
     return x;
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
  
  
  
  
  
  
   
  
