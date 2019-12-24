"use strict"

const AWS = require("aws-sdk") // eslint-disable-line import/no-extraneous-dependencies
var PromiseFtp = require("promise-ftp")

const ftp = new PromiseFtp()
const s3Stream = require("s3-upload-stream")(new AWS.S3())
const s3 = new AWS.S3()

const creds = {
  host: "ftp.ecmwf.int",
  user: "safer",
  password: "neo2008"
}

const s3Bucket = "ecmwf-files-byu-hydro-ecmwf"

function checkFileOnS3(fileName) {
  return s3
    .headObject({ Bucket: s3Bucket, Key: fileName })
    .promise()
    .then((res) => {
      // console.log(res)
      return Promise.resolve(false)
    })
    .catch((err) => {
      // console.log(err)
      // console.log("File Not found. ")
      return Promise.resolve(true)
    })
}

async function filter(arr, callback) {
  const fail = Symbol()
  return (await Promise.all(
    arr.map(async (item) => ((await callback(item)) ? item : fail))
  )).filter((i) => i !== fail)
}

async function moveFileToS3(file) {
  let size = await ftp.size(file.name)
  console.log(`Downloading file ${file.name}, size: ${size}`)
  let stream = await ftp.get(file.name)

  var upload = s3Stream.upload({
    Bucket: s3Bucket,
    Key: file.name
  })
  // upload.maxPartSize(153000000) // 150 MB
  // upload.concurrentParts(15)
  console.log("Streaming " + file.name + " To S3 bucket " + s3Bucket)

  return new Promise(function(resolve, reject) {
    stream.pipe(upload)

    upload.on("part", function(details) {
      console.log(details)
    })

    upload.on("error", (err) => {
      console.error("Error whith S3 stream")
      console.error(err)
      var myErrorObj = {
        errorType: "InternalServerError",
        httpStatus: 500,
        message: "Error whith S3 upload: " + err
      }
      stream.end()
      upload.end()
      reject(myErrorObj)
      return
    })

    upload.on("uploaded", function(details) {
      console.log(file.name + " sent to S3 bucket " + s3Bucket)
      stream.end()
      upload.end()
      resolve(details)
      return
    })
  })
}

async function main() {
  try {
    await ftp.connect({ host: creds.host, user: creds.user, password: creds.password })

    await ftp.cwd("tcyc")

    const fileList = await ftp.list()

    let filteredList = fileList.filter((file) => {
      return file.name.match(/(Runoff.*)\w+/g)
    })

    const filesNotOnS3 = await filter(filteredList, async (file) => {
      let res = await checkFileOnS3(file.name)
      return res
    })

    if (filesNotOnS3.length < 1) {
      console.log("No files to push. Quitting")
      return
    }

    let finalResult = 0
    for (let file of filesNotOnS3) {
      let result = await moveFileToS3(file)
      console.log(result)
    }

    await ftp.end()
  } catch (err) {
    console.log(err)
  }
}
main()
