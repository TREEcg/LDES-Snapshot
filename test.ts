import {fileAsStore, storeToString, stringToStore, turtleStringToStore} from "./src/util/Conversion";
import { materialize } from '@treecg/version-materialize-rdf.js';
import {DataFactory, Literal, Store} from "n3";
import {Snapshot} from "./src/Snapshot";
import namedNode = DataFactory.namedNode;
import {extractDateFromLiteral, extractTimestampFromLiteral} from "./src/util/TimestampUtil";
import literal = DataFactory.literal;
import {XSD} from "./src/util/Vocabularies";

async function run(){
    const store = await fileAsStore('versions.ttl')
    const snapshot = new Snapshot(store);
    // console.log(storeToString(snapshot.create(new Date())))
    let time:Literal = literal("2021-12-15T11:00:00.000Z",namedNode(XSD.dateTime))

    console.log('input:')
    console.log(storeToString(store))
    console.log()
    console.log('Output from snapshot method:')
    console.log(storeToString(snapshot.create(extractDateFromLiteral(time))))

}


async function ldesExample(){
    const example =`
    @prefix acl: \t<http://www.w3.org/ns/auth/acl#> .
@prefix dcterms: \t<http://purl.org/dc/terms/> .
@prefix ldes: \t<https://w3id.org/ldes#> .
@prefix ldp: \t<http://www.w3.org/ns/ldp#> .
@prefix rdf: \t<http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix tree: \t<https://w3id.org/tree#> .
@prefix xsd: \t<http://www.w3.org/2001/XMLSchema#> .
@prefix ex: \t<http://example.org/> .
@prefix owl: <https://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:ES1 a ldes:EventStream;
       ldes:versionOfPath dcterms:isVersionOf;
       ldes:timestampPath dcterms:created;
       tree:member [
           dcterms:isVersionOf <A> ;
           dcterms:created "2020-10-05T11:00:00Z"^^xsd:dateTime;
           owl:versionInfo "v0.0.1";
           rdfs:label "A v0.0.1"
       ], [
           dcterms:isVersionOf <A> ;
           dcterms:created "2020-10-06T13:00:00Z"^^xsd:dateTime;
           owl:versionInfo "v0.0.2";
           rdfs:label "A v0.0.2"
       ].`

    const store =await turtleStringToStore(example)
    const snapshot = new Snapshot(store);
    let time:Literal = literal("2020-10-05T12:00:00Z",namedNode(XSD.dateTime))

    console.log('input:')
    console.log(storeToString(store))
    console.log()
    console.log('Output from snapshot method:')
    console.log(storeToString(snapshot.create(extractDateFromLiteral(time))))


}

// run()
ldesExample()
