use dblp

/*Índice por año y autor:*/

db.documents.createIndex({'year': 1}, {'sparse': true})

db.documents.createIndex({'author': 1}, {'sparse': true})



/*1. Listado de todas las publicaciones de un autor determinado:*/

db.documents.find({"author":"Roland Meyer"}).hint('author_1')

/*2. Número de publicaciones de un autor determinado.*/

db.getCollection('documents').find({author: 'Sanjeev Saxena'}).hint('author_1').count()

/*3. Número de artículos en revista para el año 2016*/

db.documents.find({"year": 2016}).hint('year_1').count() 

/*4. Número de autores ocasionales, es decir, que tengan menos de 5 publicaciones en total.*/

db.getCollection('documents').aggregate([
    {$project: {'author': 1}},
    {$unwind : '$author' },
    {$group: {_id: '$author', total : { $sum : 1 }}},
    {$match:{'total': {'$lt': 5}}}
    ], 
    {allowDiskUse: true}
)

/*5. Número de artículos de revista (article) y número de artículos en congresos (inproceedings) de los diez autores con más publicaciones totales.*/

db.documents.aggregate([
    {$unwind: "$author"}, 
    {$group: 
        {_id: "$author", 
        count_total: {$sum:1}, 
        count_article: 
            {$sum : 
                {$cond : { if: { $eq: ["type", "article"]}, then: 1, else: 0}}
            }, 
        count_inproceedings:
            {$sum : 
                {$cond : { if: { $eq: ["type", "inproceedings"]}, then: 1, else: 0}}
            }
        }
     },
    {$sort: 
        {count_total: -1}
    },
     {$limit: 10}]
     ,{allowDiskUse: true})

/*6. Número medio de autores de todas las publicaciones que tenga en su conjunto de datos.*/


db.getCollection('documents').aggregate([
   {'$match':                                         
     {'author': {'$exists': true}}
   },
   {'$project': 
       {'author_count': { '$size': '$author' }}
   },
   {'$group':
       {
           _id: null,
           'avg_authors': {'$avg': '$author_count'}
       }
   }
])

/*7. Listado de coautores de un autor (Se denomina coautor a cualquier persona que haya firmado una publicación).*/


db.getCollection('documents').aggregate([
{$project: {'author': 1}},
{$match: {author: 'Sanjeev Saxena'}},
{$unwind : '$author' },
{$group: 
    {
        '_id': null,
        'coauthors': 
            { 
                '$addToSet': 
                {
                    "$cond": 
                    [
                        { '$ne': [ '$author', 'Sanjeev Saxena' ] },
                        '$author', null
                    ]
                }
            }
    }
},
{$project: 
    {
        _id: 0,
        coauthors: { "$setDifference": [ "$coauthors", [null] ] }
    }
}]
)


/*8. Edad de los 5 autores con un periodo de publicaciones más largo (Se considera la Edad de un autor al número de años transcurridos desde la fecha de su primera publicación hasta la última registrada).*/

db.getCollection('documents').aggregate([
    {$project: {'author': 1, 'year': 1}},
    {$unwind : '$author' },
    {$group: 
        {_id: '$author', 
         maxYear: { $max: '$year' }, 
         minYear: { $min: '$year' } 
         }
    },    
    {$project: 
        {diff: {$subtract: ['$maxYear', '$minYear']}}
    },
    {$sort: 
        {diff: -1}
    },
    {$limit: 5}
    ], 
    {allowDiskUse: true}
)

/*9. Número de autores novatos, es decir, que tengan una Edad menor de 5 años (Se considera la edad de un autor al número de años transcurridos desde 
la fecha de su primera publicación hasta la última registrada).*/

db.documents.aggregate([ 
    {$unwind: "$author" },
    {$group : { _id: "$author", anyos: { $push: "$year"} }}, 
    {$project:{ maximo: {$max: "$anyos"}, minimo:{ $min: "$anyos"} }},
    {$project: {edad: {$subtract: ["$maximo", "$minimo"] } } }, 
    {$match: {edad: {$lt: 5}}}, 
    {$count: "AutoresNovatos"} 
    ], 
    {allowDiskUse: true})


/*10. Porcentaje de publicaciones en revistas con respecto al total de publicaciones.*/

db.getCollection('documents').aggregate([
    {$project: {'type': 1}},
    {$group: 
        { 
            _id: null,
            count_article: { $sum: { $cond :  [{ $eq : ["$type", "article"]}, 1, 0]} }, 
            count_total: { $sum: 1 } 
        } 
    },
    {$project:
        {count:1, 
            percentage: 
                {$multiply: [
                    {$divide: [100, '$count_total']}, '$count_article']
                }
        }
    }], 
    {allowDiskUse: true}
)

