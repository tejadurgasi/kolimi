import com.google.gson.GsonBuilder
import play.api.libs.json._


object json {
   def main(args: Array[String]) {
     
 
  val js = "{userid=4, username=Alex, visits=1, longitude=40.7143528, latitude=-74.0059731, country=USA, city=New York CIty, gender=Male}"
   println(js)
     
    val jsonString = Json.toJson(js);
            println(jsonString) 
            val json1 = Json.stringify(jsonString)
            println(Json.stringify(jsonString)) 
             
  }

}