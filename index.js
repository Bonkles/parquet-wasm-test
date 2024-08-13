const { ParquetDataset } = require("@geoarrow/geoarrow-wasm/node");
const arrow = require('apache-arrow');

// //March building URLs
// const urls = [
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00000-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00001-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00002-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00003-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00004-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00005-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00006-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00007-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00008-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00009-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00010-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00011-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00012-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet",
//     "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00226-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet"
//   ];

const name = "part-00048-ad3ba139-0181-4cec-a708-4d76675a32b0-c000.zstd.parquet"
//const urls = [
  // "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00001-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
  // "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00007-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
  // "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00008-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
  // "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00009-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
  // "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00009-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
    //  "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-07-22.0/theme=buildings/type=building/" + name,
//];
const urlBase = `https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-07-22.0/theme=buildings/type=building/`;

const urlPart = `part-00000-ad3ba139-0181-4cec-a708-4d76675a32b0-c000.zstd.parquet`;

let urls = [];
// const urls = [
//   "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00054-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
//   "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00166-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
//   "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00167-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
//   "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00168-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
//   "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-06-13-beta.1/theme=buildings/type=building/part-00185-6bb975da-d9d5-4823-b446-c0844c96c235-c000.zstd.parquet",
// ];

for (let i=65; i<120; i++) {
  let num = pad(i, 5);
  const url = `https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-07-22.0/theme=buildings/type=building/part-${num}-ad3ba139-0181-4cec-a708-4d76675a32b0-c000.zstd.parquet`;
  urls.push(url);
}
  // const url = "https://overturemaps-us-west-2.s3.amazonaws.com/release/2024-03-12-alpha.0/theme=buildings/type=building/part-00000-4dfc75cd-2680-4d52-b5e0-f4cc9f36b267-c000.zstd.parquet"
  
  const minxPath = ["bbox", "xmin"];
  const minyPath = ["bbox", "ymin"];
  const maxxPath = ["bbox", "xmax"];
  const maxyPath = ["bbox", "ymax"];
  
  const readOptions = {
    batch_size: 1024,
    // limit: 100,
    // offset: 0,   
    // bbox: [94.9218037, 26.7301782, 94.9618037, 26.7501782],
    bbox: [3.722425034333611, 51.04693785193322, 3.7381749656686623, 51.053061945619504],
    bboxPaths: {
      minxPath,
      minyPath,
      maxxPath,
      maxyPath,
    },
  };
  
  async function readAllOvertureParquets(urls) {

    let parquetDataset = await new ParquetDataset(urlBase, [urlPart]);
  
    const wasmTable =  await parquetDataset.read(readOptions);
    
    //Read the table into JS-land
    const jsTable = arrow.tableFromIPC(wasmTable.intoIPCStream())    
    console.log(jsTable.numRows);
  }


  function pad(num, size){
      let numStr = '0000' + num;
         return numStr.substring(numStr.length-size); 
        }

  readAllOvertureParquets(urls)