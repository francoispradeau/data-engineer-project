/**
 * Ingest of movie-database csv
 * Input is single csv, output a directory that will contain files for database tables:
 * - movies
 * - genres
 * - collections
 * - production companies
 * The output is one json array containing the individual elements.
 * Further import in the db should be pretty straight forward
 */

import java.io.{File, PrintWriter}
import java.text.ParseException
import java.util.Date

import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read


object ingest {

  // Jackson JSON format
  implicit val formats: DefaultFormats.type = DefaultFormats

  // Columns-of-interest mapping for the movie-database.csv
  val ID_COL_ID = 5
  val TITLE_COL_ID = 20
  val RELEASE_DATE_COL_ID = 14
  val REVENUE_DATE_COL_ID = 15
  val BUDGET_COL_ID = 2
  val POPULARITY_COL_ID = 10
  val COLLECTION_COL_ID = 1
  val GENRE_COL_ID = 3
  val PRODCOMPANY_COL_ID = 12
  val NUM_COLS = 24

  // Objects that will mirror the db objects that we will extract data into
  // and write separate files for
  case class Movie(id: Int,
                   title: String,
                   release_date: Date,
                   revenue: Double,
                   budget: Double,
                   popularity: Double,
                   collectionsId: List[Int],
                   genresId: List[Int],
                   productionCompaniesId: List[Int]
                  )

  case class Genre(id: Int, name: String)

  case class Collection(id: Int, name: String)

  case class ProductionCompany(id: Int, name: String)


  /**
   * Main - takes input movie-database.csv and output directory that will contain files for movies, genres,
   *  collections and production companies
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 2 arguments. Note: might want to use a cmd line parsing library but limiting
    //  dependencies for now
    if (args.size < 2) {
      println("Usage: ingest <filePath> <outputdir>.")
      System.exit(1)
    }
    // Input file and output directory
    val moviesFilename = args(0)
    val outputPath = args(1)
    // Input file validation
    val moviesFile = new File(moviesFilename)
    if (!moviesFile.exists()) {
      println("File %s does not exist.".format(moviesFilename))
      System.exit(1)

    }

    // We'll keep separate sets for the db objects
    var movies = scala.collection.mutable.Set[Movie]()
    var genres = scala.collection.mutable.Set[Genre]()
    var collections = scala.collection.mutable.Set[Collection]()
    var prodCompanies = scala.collection.mutable.Set[ProductionCompany]()

    // The import is performed. Returned tuple is (total # of lines, # of lines dropped). Drops are due to parsing
    //   errors
    val stats : Tuple2[Int,Int] = readDatabaseData(moviesFile, movies, genres, collections, prodCompanies)

    println("Read total of %d lines, dropping %d lines. Success percent is %f".format(stats._1, stats._2,
      (stats._1-stats._2).toFloat/stats._1.toFloat))

    // Write 4 files with the json for the objects.
    writeCollection(outputPath + "/genres.json", genres)
    writeCollection(outputPath + "/collections.json", collections)
    writeCollection(outputPath + "/companies.json", prodCompanies)
    writeCollection(outputPath + "/movies.json", movies)

    // DONE
    System.exit(0)
  }

  /**
   * Importing the data from moviesFile File. It updates the passed sets of Movie, Genre, Collection and
   * ProductionCompany. Returned is a Tuple2 summarizing the import: (total # of lines, # of lines dropped)
   * @param moviesFile
   * @param movies
   * @param genres
   * @param collections
   * @param prodCompanies
   * @return
   */
  def readDatabaseData(moviesFile: File, movies: mutable.Set[Movie],
                       genres: mutable.Set[Genre], collections: mutable.Set[Collection],
                       prodCompanies: mutable.Set[ProductionCompany]) : Tuple2[Int,Int] = {
    // Start reading
    val bufferedSource = io.Source.fromFile(moviesFile)

    var badLines = mutable.Set[String]()
    var nLines : Int = 0
    // Dropping first line (headers)
    for (line <- bufferedSource.getLines.drop(1)) {
      nLines += 1
      // Extract columns. The csv has commas in quoted text, so need the regex approach
      val cols: Array[String] = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)

      // Check for correct splitting
      if (cols.length != NUM_COLS) {
        println("Line dropped - invalid number of columns: %s".format(line))
        badLines += line
      } else {
        // Create empty default sets from within this particular line.
        var tempGenres = Set[Genre]()
        var tempCollections = Set[Collection]()
        var tempCompanies = Set[ProductionCompany]()

        // Convert into db objects. Note this check will avoid duplicates
        if (movies.find(_.id == cols(ID_COL_ID).toInt).isEmpty) {

          // Collect derived types first
          val genreStr = formatStringFromCsv(cols(GENRE_COL_ID))
          try {
            if ( genreStr.length > 1 ) tempGenres = read[Array[Genre]](genreStr).toSet
          } catch {
            // Couldn't get genres.
            case _: Throwable => println("Failed to read genres from %s".format(genreStr))
          }
          // Collections
          val collStr = formatStringFromCsv(cols(COLLECTION_COL_ID))
          try {
            if ( collStr.length > 1 ) tempCollections = read[Array[Collection]](collStr).toSet
          } catch {
            case _: Throwable => println("Failed to read collections from %s".format(collStr))
          }
          // Companies
          val compStr = formatStringFromCsv(cols(PRODCOMPANY_COL_ID))
          try {
            if ( compStr.length > 1 ) tempCompanies = read[Array[ProductionCompany]](compStr).toSet
          } catch {
            case _: Throwable => println("Failed to read companies from %s".format(compStr))
          }

          // Create new movie and add it to the set if no error occurred
          val newMovie = createMovieFromData(tempGenres, tempCollections, tempCompanies, cols)
          if (newMovie.isDefined) {
            movies += newMovie.get
            genres ++= tempGenres
            collections ++= tempCollections
            prodCompanies ++= tempCompanies
          } else {
            println("Could not add movie %s".format(line))
            badLines += line
          }
        } else {
          println("Movie already ingested: %s".format(line))
        }
      }
    }
    // Close input file
    bufferedSource.close
    (nLines, badLines.size)
  }

  /**
   * formatStringFromCsv: takes the raw string and 'processes' it based on data 'issues' that were found
   * in parsing the example csv.
   * @param input
   * @return
   */
  def formatStringFromCsv(input: String): String = {
    // Here we are thinking double quotes are already within single quotes. We're replacing them with
    //   single quotes as it is one way to have quotes within quotes
    var tempStr = input.replaceAllLiterally("\"\"", "\'")

    // Hacking away as often with CSV... single quotes are non-standard for
    //   json4 and we need to replace them carefully
    import java.util.regex.Pattern
    val pattern = "((?<=(\\{|\\[|\\,|:))\\s*')|('\\s*(?=(\\}|(\\])|(\\,|:))))".r
    val p = Pattern.compile("((?<=(\\{|\\[|\\,|:))\\s*')|('\\s*(?=(\\}|(\\])|(\\,|:))))")
    val replace = "\""
    val m = p.matcher(tempStr)
    tempStr = m.replaceAll(replace)

    // Seems like there are double quotes and None is used as 'nil'
    tempStr = tempStr.replaceAllLiterally(": None", ": \"\"")
    if (tempStr.length < 1) {
      return tempStr
    }
    // Remove extra quotes around the text if any
    var cleaned = tempStr
    if ( tempStr.length > 2 && tempStr(0) == '\"' && tempStr(tempStr.length-1) == '\"' )
    {
      cleaned = tempStr.substring(1, tempStr.length - 1)
    }
    if (cleaned.length < 1) {
      return cleaned
    }
    // Now check if this is an array or just a single element. Always wrap
    //  into an array for single json objects
    if (cleaned(0) == '{') {
      return '[' + cleaned + ']'
    }
    cleaned

  }

  /**
   * Write a collection to a file.
   * @param name
   * @param collection
   * @tparam T
   */
  def writeCollection[T](name: String, collection: mutable.Set[T]): Unit = {
    val collectionFile = new File(name)
    if (!collectionFile.exists) {
      println("Creating new file")
      collectionFile.createNewFile
    }
    // Straight write. Note that this could be prettified if need be...
    val pw = new PrintWriter(collectionFile)
    pw.write(Serialization.write(collection))
    pw.close

  }

  /**
   * creates the Movie object
   * @param genres: genres Set to fill in the Movie object (id only)
   * @param collections: collections Set to fill in the Movie object (id only)
   * @param prodCompanies: : production companies Set to fill in the Movie object (id only)
   * @param cols: all columns parsed from the csv line
   * @return: An option that will be None if some data extraction/transformation failed.
   */
  def createMovieFromData(genres: Set[Genre], collections: Set[Collection],
                          prodCompanies: Set[ProductionCompany], cols: Array[String]): Option[Movie] = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    try {
      val relDate = format.parse(cols(RELEASE_DATE_COL_ID))
      // Create object.
      val newMovie = Movie(cols(ID_COL_ID).toInt, cols(TITLE_COL_ID), relDate, cols(REVENUE_DATE_COL_ID).toDouble,
        cols(BUDGET_COL_ID).toDouble, cols(POPULARITY_COL_ID).toDouble, collections.map(_.id).toList,
        genres.map(_.id).toList, prodCompanies.map(_.id).toList)
      return Some(newMovie)

    } catch {
      case e: ParseException => {
        println("Date formatting exception")
        return None
      }
      case e: NumberFormatException => {
        println("Number formatting exception")
        return None
      }
      case e: UnknownError => {
        println("Other error")
        return None
      }
    }

  }

}
