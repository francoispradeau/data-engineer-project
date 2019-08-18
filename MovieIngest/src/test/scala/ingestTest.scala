import ingest.Genre
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.scalatest.FunSuite

class ingestTest extends FunSuite {

  // Jackson JSON format
  implicit val formats: DefaultFormats.type = DefaultFormats

  test("The json for line should be split in columns") {
    // From the raw line
    val line = """False,"{'id': 10194, 'name': 'Toy Story Collection', 'poster_path': '/7G9915LfUQ2lVfwMEEhDsn3kT4B.jpg', 'backdrop_path': '/9FBwqcd9IRruEDUrTdcaafOMKUq.jpg'}",30000000,"[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]",http://toystory.disney.com/toy-story,862,tt0114709,en,Toy Story,"Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.",21.946943,/rhIRbceoE9lR4veEXuwCC2wARtG.jpg,"[{'name': 'Pixar Animation Studios', 'id': 3}]","[{'iso_3166_1': 'US', 'name': 'United States of America'}]",1995-10-30,373554033,81.0,"[{'iso_639_1': 'en', 'name': 'English'}]",Released,,Toy Story,False,7.7,5415"""
    val cols: Array[String] = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
    assert(cols.length == ingest.NUM_COLS)

    val test2 = ingest.formatStringFromCsv(cols(ingest.GENRE_COL_ID))
    assert(read[Array[Genre]](test2).length == 3)
  }

  test("The json for Genre should be deserialized") {
    // Just deserializing Genre
    val test1 = """[{"id": 16, "name": "Animation"}, {"id": 35, "name": "Comedy"}, {"id": 10751, "name": "Family"}]"""
    assert(read[Array[Genre]](test1).toSet.size == 3)
  }

  test("double quotes should be changed to single ones within quotes") {

    val test1 = """{'name': 'Zespól Filmowy ""Tor""', 'id': 7984}"""
    val test2 = ingest.formatStringFromCsv(test1)
    assert(test2 == """[{"name":"Zespól Filmowy 'Tor'","id": 7984}]""")
    assert(read[Array[Genre]](test2).toSet.size == 1)
  }

}
