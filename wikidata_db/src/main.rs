use bzip2::read::BzDecoder;
use clap::Parser;
use csv::Writer;
use indicatif::ProgressBar;
use json::JsonValue;
use json::stringify;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::sync::Arc;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use thiserror::Error;
use threadpool::ThreadPool;

enum Message {
    Data(Data),
    Error(DataExtractionError),
    End
}

#[derive(Debug,Default)]
struct Data {
    qid: String,
    labels: HashMap<String, Vec<String>>,
    description: HashMap<String, String>,
    properties: HashMap<String, Vec<String>>,
    data_props: HashMap<String, Vec<Vec<String>>>,
    wiki: HashMap<String, String>,
}

#[derive(Parser, Debug)]
#[command(about="Wikidata JSON Loader for Teanga Entity Extraction")]
struct Opts {
    #[arg(short, long, default_value="en")]
    lang: Vec<String>,
    #[arg(short, long, default_value="latest-all.json.bz2")]
    wikidata: String,
    #[arg(short, long, default_value="5")]
    num_threads: usize,
}

#[derive(Error, Debug)]
enum DataExtractionError {
    #[error("JSON object on line {1} does not meet expectation: {0}")]
    JsonModel(String, usize),
    #[error("JSON parsing error: {0}")]
    JsonParse(#[from] json::Error),
}

fn process_claim(values : &JsonValue, line_no : usize) -> Result<(Vec<String>, Vec<Vec<String>>), DataExtractionError> {
    let mut properties = Vec::new();
    let mut data_props = Vec::new();
    if let JsonValue::Array(values) = values {
        for v in values.iter() {
            if let JsonValue::Object(v) = v {
                if let Some(JsonValue::Object(mainsnak)) = v.get("mainsnak") {
                    if let Some(JsonValue::Object(datavalue)) = mainsnak.get("datavalue") {
                        if let Some(datatype) = mainsnak.get("datatype") {
                            if !datatype.is_string() {
                                return Err(DataExtractionError::JsonModel("Datatype is not a string".to_string(), line_no));
                            }
                            if datatype.as_str().unwrap() == "wikibase-item" {
                                if let Some(JsonValue::Object(value)) = datavalue.get("value") {
                                    if let Some(id) = value.get("id") {
                                        if !id.is_string() {
                                            return Err(DataExtractionError::JsonModel("Id is not a string".to_string(), line_no));
                                        }
                                        properties.push(id.as_str().unwrap().to_string());
                                    } else {
                                        return Err(DataExtractionError::JsonModel("No id field in value".to_string(), line_no));
                                    }
                                } else {
                                    return Err(DataExtractionError::JsonModel("No value field in datavalue".to_string(), line_no));
                                }
                            } else if datatype.as_str().unwrap() == "string" {
                                if let Some(value) = datavalue.get("value") {
                                    if !value.is_string() {
                                        return Err(DataExtractionError::JsonModel("Value is not a string".to_string(), line_no));
                                    }
                                    data_props.push(vec![value.as_str().unwrap().to_string()]);
                                } else {
                                    return Err(DataExtractionError::JsonModel("No value field in datavalue".to_string(), line_no));
                                }
                            } else if datatype.as_str().unwrap() == "time" {
                                if let Some(JsonValue::Object(value)) = datavalue.get("value") {
                                    if let Some(time) = value.get("time") {
                                        if !time.is_string() {
                                            return Err(DataExtractionError::JsonModel("Time is not a string".to_string(), line_no));
                                        }
                                        data_props.push(vec![time.as_str().unwrap().to_string(), datatype.as_str().unwrap().to_string()]);
                                    } else {
                                        return Err(DataExtractionError::JsonModel("No time field in value".to_string(), line_no));
                                    }
                                }
                            }
                        } else {
                            return Err(DataExtractionError::JsonModel("No datatype field in mainsnak".to_string(), line_no));
                        }
                    }
                } else {
                    return Err(DataExtractionError::JsonModel("No mainsnak field".to_string(), line_no));
                }
            } else {
                return Err(DataExtractionError::JsonModel("Values not an object".to_string(), line_no));
            }

        }
    } else {
        return Err(DataExtractionError::JsonModel("Values not an array".to_string(), line_no));
    }
    Ok((properties, data_props))

}

fn extract_data(json : &JsonValue, langs : Arc<Vec<String>>, line_no : usize) -> Result<Data, DataExtractionError> {
    let mut data = Data::default();
    match json {
        JsonValue::Object(obj) => {
            data.qid = obj.get("id").ok_or(DataExtractionError::JsonModel("No id field".to_string(), line_no))?.as_str().unwrap().to_string();
            
            if let Some(JsonValue::Object(labels)) = obj.get("labels") {
                for l in langs.iter() {
                    if let Some(JsonValue::Object(label)) = labels.get(l) {
                        data.labels.entry(l.to_string()).or_insert(Vec::new())
                            .push(label.get("value")
                                .ok_or(DataExtractionError::JsonModel("No value field".to_string(), line_no))?
                                .as_str().unwrap().to_string());
                    } else if let Some(JsonValue::Object(label)) = labels.get("mul") {
                        data.labels.entry(l.to_string()).or_insert(Vec::new())
                            .push(label.get("value")
                                .ok_or(DataExtractionError::JsonModel("No value field".to_string(), line_no))?
                                .as_str().unwrap().to_string());
                    } 
                }
            } else {
                return Err(DataExtractionError::JsonModel("No labels field".to_string(), line_no));
            }
            
            if let Some(JsonValue::Object(aliases)) = obj.get("aliases") {
                for l in langs.iter() {
                    if let Some(JsonValue::Array(alias)) = aliases.get(l) {
                        for a in alias.iter() {
                            if let JsonValue::Object(alias) = a {
                                data.labels.entry(l.to_string()).or_insert(Vec::new())
                                    .push(alias.get("value")
                                        .ok_or(DataExtractionError::JsonModel("No value field".to_string(), line_no))?
                                        .as_str().unwrap().to_string());
                            } else {
                                return Err(DataExtractionError::JsonModel("Aliases not an array".to_string(), line_no));
                            }
                        }
                    } else if let Some(JsonValue::Array(alias)) = aliases.get("mul") {
                        for a in alias.iter() {
                            if let JsonValue::Object(alias) = a {
                                data.labels.entry(l.to_string()).or_insert(Vec::new())
                                    .push(alias.get("value")
                                        .ok_or(DataExtractionError::JsonModel("No value field".to_string(), line_no))?
                                        .as_str().unwrap().to_string());
                            } else {
                                return Err(DataExtractionError::JsonModel("Aliases not an array".to_string(), line_no));
                            }
                        }
                    }
                }
            }

            if let Some(JsonValue::Object(descriptions)) = obj.get("descriptions") {
                for l in langs.iter() {
                    if let Some(JsonValue::Object(desc)) = descriptions.get(l) {
                        data.description.insert(l.to_string(), desc.get("value").unwrap().as_str().unwrap().to_string());
                    } else if let Some(JsonValue::Object(desc)) = descriptions.get("mul") {
                        data.description.insert(l.to_string(), desc.get("value").unwrap().as_str().unwrap().to_string());
                    }
                }
            } else {
                return Err(DataExtractionError::JsonModel("No descriptions field".to_string(), line_no));
            }

            if let Some(JsonValue::Object(claims)) = obj.get("claims") {
                for (prop, values) in claims.iter() {
                    let (props, data_props) = process_claim(values, line_no)?;
                    if props.len() > 0 {
                        data.properties.insert(prop.to_string(), props);
                    }
                    if data_props.len() > 0 {
                        data.data_props.insert(prop.to_string(), data_props);
                    }
                }
            } else {
                return Err(DataExtractionError::JsonModel("No claims field".to_string(), line_no));
            }

            if let Some(JsonValue::Object(sitelinks)) = obj.get("sitelinks") {
                for l in langs.iter() {
                    if let Some(JsonValue::Object(sitelink)) = sitelinks.get(&format!("{}wiki", l)) {
                        data.wiki.insert(l.to_string(), sitelink.get("title")
                            .ok_or(DataExtractionError::JsonModel("No title field".to_string(), line_no))?
                            .as_str().unwrap().to_string());
                    }
                }
            }

            Ok(data)
        },
        _ => {
            Err(DataExtractionError::JsonModel("Root object is not a JSON object".to_string(), line_no))
        }
    }
}

fn read_wikidata(filename : String, num_threads: usize, 
    lang : Arc<Vec<String>>, tx : Sender<Message>) {
    let file = File::open(&filename).expect(&format!("Could not open file {}", filename));
    let decompressor = BzDecoder::new(file);
    let reader = BufReader::new(decompressor);
    let pool = ThreadPool::new(num_threads);
    let mut line_no = 0;
    let pbar = ProgressBar::no_length();

    for line in reader.lines() {
        line_no += 1;
        let line = line.expect("Could not read line");
        if line.len() < 2 {
            continue;
        }
        let tx = tx.clone();
        let l = lang.clone();
        let pbar = pbar.clone();
        pool.execute(move || {
            match json::parse(&line[..line.len() - 1]) {
                Ok(json) => {
                    match extract_data(&json, l, line_no) {
                        Ok(data) => {
                            tx.send(Message::Data(data)).expect("Could not send data");
                        },
                        Err(err) => {
                            tx.send(Message::Error(err)).expect("Could not send error");
                        }
                    }
                },
                Err(err) => {
                    tx.send(Message::Error(DataExtractionError::JsonParse(err))).expect("Could not send error");
                }
            }
            pbar.inc(1);
        });

    }
    pool.join();
    tx.send(Message::End).expect("Could not send end message");
}

fn write_data(rx : Receiver<Message>, lang : Arc<Vec<String>>) {
    let mut label_writers = HashMap::new();
    for l in lang.iter() {
        let mut writer = Writer::from_path(format!("labels_{}.csv", l)).expect("Could not open labels.csv");
        writer.write_record(&["qid", "label"]).expect("Could not write record");
        label_writers.insert(l.to_string(), writer);
    }
    let mut desc_writers = HashMap::new();
    for l in lang.iter() {
        let mut writer = Writer::from_path(format!("descriptions_{}.csv", l)).expect("Could not open descriptions.csv");
        writer.write_record(&["qid", "description"]).expect("Could not write record");
        desc_writers.insert(l.to_string(), writer);
    }
    let mut prop_writer = Writer::from_path("properties.csv").expect("Could not open properties.csv");
    prop_writer.write_record(&["qid", "properties"]).expect("Could not write record");
    let mut data_prop_writer = Writer::from_path("data_properties.csv").expect("Could not open data_properties.csv");
    data_prop_writer.write_record(&["qid", "data_properties"]).expect("Could not write record");
    let mut wiki_writers = HashMap::new();
    for l in lang.iter() {
        let mut writer = Writer::from_path(format!("wiki_{}.csv", l)).expect("Could not open wiki.csv");
        writer.write_record(&["qid", "wiki"]).expect("Could not write record");
        wiki_writers.insert(l.to_string(), writer);
    }
    loop {
        match rx.recv() {
            Ok(Message::Data(data)) => {
                for (l, labels) in data.labels.iter() {
                    let writer = label_writers.get_mut(l).expect("Could not get label writer");
                    writer.write_record(&[data.qid.as_str(), stringify(labels.clone()).as_str()]).expect("Could not write record");
                }
                for (l, desc) in data.description.iter() {
                    let writer = desc_writers.get_mut(l).expect("Could not get description writer");
                    writer.write_record(&[data.qid.as_str(), desc.as_str()]).expect("Could not write record");
                }
                prop_writer.write_record(&[data.qid.as_str(), stringify(data.properties.clone()).as_str()]).expect("Could not write record");
                data_prop_writer.write_record(&[data.qid.as_str(), stringify(data.data_props.clone()).as_str()]).expect("Could not write record");
                for (l, wiki) in data.wiki.iter() {
                    let writer = wiki_writers.get_mut(l).expect("Could not get wiki writer");
                    writer.write_record(&[data.qid.as_str(), wiki.as_str()]).expect("Could not write record");
                }
            },
            Ok(Message::End) => {
                break;
            },
            Ok(Message::Error(err)) => {
                println!("Error: {:?}", err);
            },
            Err(_) => {
                break;
            }
        }
    }
}


fn main() {
    let args = Opts::parse();
    let wikidata = args.wikidata.clone();
    let lang = Arc::new(args.lang.clone());
    let (tx, rx) = channel();
    let lang1 = lang.clone();
    let lang2 = lang.clone();
    let t1 = thread::spawn(move || {
        read_wikidata(wikidata, args.num_threads, lang1, tx);
    });
    let t2 = thread::spawn(move || {
        write_data(rx, lang2);
    });
    t1.join().unwrap();
    t2.join().unwrap();
}
