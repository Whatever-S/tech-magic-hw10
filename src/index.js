import { connect, close } from './connection.js';

const db = await connect();
const usersCollection = db.collection("users");
const articlesCollection = db.collection("articles")
const studentsCollection = db.collection("students")

const run = async () => {
  try {
    // await getUsersExample();
    // await task1();
    // await task2();
    // await task3();
    // await task4();
    // await task5();
    // await task6();
    // await task7();
    // await task8();
    // await task9();
    // await task10();
    // await task11();
    // await task12();

    await close();
  } catch(err) {
    console.log('Error: ', err)
  }
}
run();

// #### Users
// - Get users example
async function getUsersExample () {
  try {
    const [allUsers, firstUser] = await Promise.all([
      usersCollection.find().toArray(),
      usersCollection.findOne(),
    ])

    console.log('allUsers', allUsers);
    console.log('firstUser', firstUser);
  } catch (err) {
    console.error('getUsersExample', err);
  }
}

// - Get all users, sort them by age (ascending), and return only 5 records with firstName, lastName, and age fields.
async function task1 () {
  try {
    const allUsers = await usersCollection.find( {},  
      { projection: {firstName: 1, lastName: 1, age: 1, _id: 0}
      }).limit(5).sort({age : "asc"  }).toArray()
    console.log(allUsers)
  } catch (err) {
    console.error('task1', err)
  }
}

// - Add new field 'skills: []" for all users where age >= 25 && age < 30 or tags includes 'Engineering'
async function task2 () {
  try {
    const usersWithSkills = await usersCollection.updateMany(
      { $or: [{ age: { $gte: 25, $lt: 30 } }, { tags: 'Engineering' }] }  ,
      { $set: {skills: []} }
    )
    console.log(usersWithSkills)
  } catch (err) {
    console.error('task2', err)
  }
}

// - Update the first document and return the updated document in one operation (add 'js' and 'git' to the 'skills' array)
//   Filter: the document should contain the 'skills' field
async function task3() {
  try {
    const updatedDocument = await usersCollection.findOneAndUpdate(
      { skills: { $exists: true } },
      { $push: { skills: { $each: ['js', 'git'] } } },
      { returnDocument: "after" }
    )
    console.log(updatedDocument)
  } catch (err) {
    console.error('task3', err)
  }
}

// - REPLACE the first document where the 'email' field starts with 'john' and the 'address state' is equal to 'CA'
//   Set firstName: "Jason", lastName: "Wood", tags: ['a', 'b', 'c'], department: 'Support'
async function task4 () {
  try {
    const filter = {
      email: /^john/,
      "address.state": "CA"
    };

    const replacement = {
      firstName: "Jason",
      lastName: "Wood",
      tags: ['a', 'b', 'c'],
      department: 'Support'
    };

    const replacedDocument = await usersCollection.findOneAndReplace(filter, replacement, { returnDocument: "after" });
    console.log(replacedDocument);
  } catch (err) {
    console.log('task4', err);
  }
}

// - Pull tag 'c' from the first document where firstName: "Jason", lastName: "Wood"
async function task5 () {
  try {
    const updatedDocument = await usersCollection.findOneAndUpdate(
      { firstName: "Jason", lastName: "Wood" },
      { $pull:  { tags: 'c' }},
      { returnDocument: "after" }
    )
    console.log(updatedDocument);
  } catch (err) {
    console.log('task5', err);
  }
}

// - Push tag 'b' to the first document where firstName: "Jason", lastName: "Wood"
//   ONLY if the 'b' value does not exist in the 'tags'
async function task6 () {
  try {
    const updatedDocument = await usersCollection.findOneAndUpdate(
      { firstName: "Jason", lastName: "Wood" , tags: { $nin: ['b'] }},
      { $push:  { tags: 'b' }},
      { returnDocument: "after" }
    )
    console.log(updatedDocument);
  } catch (err) {
    console.log('task6', err);
  }
}

// - Delete all users by department (Support)
async function task7 () {
  try {
    const updatedDocuments = await usersCollection.deleteMany(
      {department: 'Support'}
    )
    console.log(updatedDocuments)
  } catch (err) {
    console.log('task7', err);
  }
}

// #### Articles
// - Create new collection 'articles'. Using bulk write:
//   Create one article per each type (a, b, c)
//   Find articles with type a, and update tag list with next value ['tag1-a', 'tag2-a', 'tag3']
//   Add tags ['tag2', 'tag3', 'super'] to articles except articles with type 'a'
//   Pull ['tag2', 'tag1-a'] from all articles
async function task8 () {
  try {
    const bulkOperations = [];
    const articles = [
      { name: 'Mongodb - introduction', description: 'Mongodb - text', type: 'a', tags: [] },
      { name: 'MySQL - introduction', description: 'MySQL - text', type: 'b', tags: [] },
      { name: 'PostgreSQL  - introduction', description: 'PostgreSQL - text', type: 'c', tags: [] }
    ];

    // Create articles
    articles.forEach((article) => {
      bulkOperations.push({
        insertOne: { document: article }
      });
    });

    // Find and update articles with type 'a' and update tag list
    const updateFilter = { type: 'a' };
    const updateOperation = {
      $set: {
        tags: ['tag1-a', 'tag2-a', 'tag3']
      }
    };
    bulkOperations.push({
      updateMany: {
        filter: updateFilter,
        update: updateOperation
      }
    });

    // Add tags ['tag2', 'tag3', 'super'] to articles except type 'a'
    const addTagsFilter = { type: { $ne: 'a' } };
    const addTagsOperation = {
      $push: {
        tags: { $each: ['tag2', 'tag3', 'super'] }
      }
    };
    bulkOperations.push({
      updateMany: {
        filter: addTagsFilter,
        update: addTagsOperation
      }
    });

    // Pull tags ['tag2', 'tag1-a'] from all articles
    const pullTagsOperation = {
      $pull: {
        tags: { $in: ['tag2', 'tag1-a'] }
      }
    };
    bulkOperations.push({
      updateMany: {
        filter: {},
        update: pullTagsOperation
      }
    });

    const bulkWriteResult = await articlesCollection.bulkWrite(bulkOperations);
    console.log('Bulk write result:', bulkWriteResult);
  } catch (err) {
    console.error('task8', err);
  }
}

// - Find all articles that contains tags 'super' or 'tag2-a'
async function task9 () {
  try {
    const resultArticles = await articlesCollection.find(
      {tags: { $in: ['super', 'tag2-a']}}
    ).toArray()
    console.log(resultArticles)
  } catch (err) {
    console.log('task9', err);
  }
}

// #### Students Statistic (Aggregations)
// - Find the student who have the worst score for homework, the result should be [ { name: <name>, worst_homework_score: <score> } ]
async function task10 () {
  try {
    const pipeline = [
      {
        $unwind: "$scores"
      },
      {
        $match: {
          "scores.type": "homework"
        }
      },
      {
        $group: {
          _id: "$name",
          worst_homework_score: { $min: "$scores.score" }
        }
      },
      {
        $sort: { worst_homework_score: 1 }
      },
      {
        $limit: 1
      },
      {
        $project: {
          _id: 0,
          name: "$_id",
          worst_homework_score: 1
        }
      }
    ];
    const result = await studentsCollection.aggregate(pipeline).toArray();
    console.log('Student with the worst homework score:', result);
  } catch (err) {
    console.log('task10', err);
  } 
}

// - Calculate the average score for homework for all students, the result should be [ { avg_score: <number> } ]
async function task11 () {
  try {
    const pipeline = [
      {
        $unwind: "$scores"
      },
      {
        $match: {
          "scores.type": "homework"
        }
      },
      {
        $group: {
          _id: null,
          avg_score: { $avg: "$scores.score" }
        }
      },
      {
        $project: {
          _id: 0,
          avg_score: 1
        }
      }
    ];

    const avgScore = await studentsCollection.aggregate(pipeline).toArray();
    console.log('Average score for homework:', avgScore);
  } catch (err) {
    console.log('task11', err);
  } 
}

// - Calculate the average score by all types (homework, exam, quiz) for each student, sort from the largest to the smallest value
async function task12 () {
  try {
    const pipeline = [
      {
        $unwind: "$scores"
      },
      {
        $group: {
          _id: "$name",
          avg_score: {
            $avg: "$scores.score"
          }
        }
      },
      {
        $sort: { avg_score: -1 }
      },
      {
        $project: {
          _id: 0,
          name: "$_id",
          avg_score: 1
        }
      }
    ];
    const stutentsList = await studentsCollection.aggregate(pipeline).toArray();
    console.log('Average scores by type:', stutentsList);
  } catch (err) {
    console.log('task12', err);
  } 
}
