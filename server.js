/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
require('dotenv').config();
var knex = require('knex');
const imageThumbnail = require('image-thumbnail');

module.exports.triggerETL = async (limit = 10) => {
  console.log(`Converting the next ${limit} photos`);
  
  const db = knex({
    client: 'mssql',
    connection: {
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME,
      options: {
        enableArithAbort: true,
      }
    },
    useNullAsDefault: true 
  });

  const photos = await db.select('RowId', 'File') 
    .from('dbo.Photo as PH')
    .whereNull('PH.ThumbFile')
    .orderBy('PH.RowId', 'asc')
    .limit(limit);

  let options = { percentage: 66, jpegOptions: { force: true, quality: 33 } };
  for (const photo of photos) {    
    const ThumbFile = await imageThumbnail(photo.File, options);
    const body = { ThumbFile }

    await db('dbo.photo')
    .update(body)
    .where('RowId', photo.RowId);
    console.log("ROW_ID", photo.RowId)
  }

  console.log("success");
  process.exit();
};
