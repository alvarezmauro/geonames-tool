const fs = require("fs");
const csv = require("csv-parser");

function replaceSingleQuotes(str) {
  if (!str) return str;
  return str.replace(/'/g, "''");
}

// Function to create the SQL insert command
function createInsertSQL(cityData) {
  const [
    geonameid,
    name,
    asciiname,
    alternatenames,
    latitude,
    longitude,
    featureClass,
    featureCode,
    countryCode,
    cc2,
    admin1Code,
    admin2Code,
    admin3Code,
    admin4Code,
    population,
    elevation,
    dem,
    timezone,
    modificationDate,
  ] = cityData;

  return `INSERT INTO cities (geonameid, name, asciiname, alternatenames, latitude, longitude, featureClass, featureCode, countryCode, cc2, admin1Code, admin2Code, admin3Code, admin4Code, population, elevation, dem, timezone, modificationDate) VALUES (${geonameid}, '${replaceSingleQuotes(
    name
  )}', '${replaceSingleQuotes(asciiname)}', '${replaceSingleQuotes(
    alternatenames
  )}', ${latitude}, ${longitude}, '${featureClass}', '${featureCode}', '${countryCode}', '${cc2}', '${replaceSingleQuotes(
    admin1Code
  )}', '${replaceSingleQuotes(admin2Code)}', '${replaceSingleQuotes(
    admin3Code
  )}', '${replaceSingleQuotes(admin4Code)}', ${population || "NULL"}, ${
    elevation || "NULL"
  }, ${dem || "NULL"}, '${replaceSingleQuotes(
    timezone
  )}', '${modificationDate}');\n`;
}

// Path to the cities15000.txt data file
const inputFile = "cities15000.txt";

// Output file name for the SQL dump
const outputFile = "cities.sql";
const writeStream = fs.createWriteStream(outputFile, { encoding: "utf8" });
const readStream = fs
  .createReadStream(inputFile, { encoding: "utf8" })
  .pipe(
    csv({
      separator: "\t",
      headers: [
        "geonameid",
        "name",
        "asciiname",
        "alternatenames",
        "latitude",
        "longitude",
        "featureClass",
        "featureCode",
        "countryCode",
        "cc2",
        "admin1Code",
        "admin2Code",
        "admin3Code",
        "admin4Code",
        "population",
        "elevation",
        "dem",
        "timezone",
        "modificationDate",
      ],
    })
  )
  .on("data", (line) => {
    const data = Object.values(line);
    if (data.length !== 19) {
      console.log("City data is not complete:")
      console.log("data", data);
    } else {
      const insertSQL = createInsertSQL(data);
      writeStream.write(insertSQL);
    }
  });

// When the reading is finished, close the write stream
readStream.on("end", () => {
  writeStream.end();
});

// Handle errors
readStream.on("error", (err) => {
  console.error("Error reading the input file:", err);
});

writeStream.on("error", (err) => {
  console.error("Error writing to the output file:", err);
});

writeStream.on("finish", () => {
  console.log("SQL dump file created successfully!");
});
