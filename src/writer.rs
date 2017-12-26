use std::collections::HashMap;
use std::io::Write;
use std::iter::once;
use std::rc::Rc;

use failure::{Error, err_msg};
use libflate::deflate::Encoder;
use rand::random;
use serde_json;
#[cfg(feature = "snappy")] use snap::Writer as SnappyWriter;

use encode::EncodeAvro;
use schema::{Name, Schema};
use types::{ToAvro, Value};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Codec {
    Null,
    Deflate,
    #[cfg(feature = "snappy")] Snappy,
}

impl ToAvro for Codec {
    fn avro(self) -> Value {
        Value::Bytes(
            match self {
                Codec::Null => "null",
                Codec::Deflate => "deflate",
                #[cfg(feature = "snappy")] Codec::Snappy => "snappy",
            }
                .to_owned().into_bytes())
    }
}

pub struct Writer<'a, W> {
    schema: &'a Schema,
    writer: W,
    codec: Codec,
    marker: Vec<u8>,
    has_header: bool,
}

impl<'a, W: Write> Writer<'a, W> {
    pub fn new(schema: &'a Schema, writer: W) -> Writer<'a, W> {
        Self::with_codec(schema, writer, Codec::Null)
    }

    pub fn with_codec(schema: &'a Schema, writer: W, codec: Codec) -> Writer<'a, W> {
        let mut marker = Vec::with_capacity(16);
        for _ in 0..16 {
            marker.push(random::<u8>());
        }

        Writer {
            schema: schema,
            writer: writer,
            codec: codec,
            marker: marker,
            has_header: false,
        }
    }

    pub fn header(&mut self) -> Result<usize, Error> {
        let magic_schema = Schema::Fixed { name: Name::new("Magic"), size: 4 };
        let meta_schema = &Schema::Map(Rc::new(Schema::Bytes));
        let mut metadata = HashMap::new();
        metadata.insert("avro.schema", Value::Bytes(serde_json::to_string(self.schema)?.into_bytes()));
        metadata.insert("avro.codec", self.codec.avro());

        Ok(self.append_raw(&magic_schema, &['O' as u8, 'b' as u8, 'j' as u8, 1u8][..])? +
               self.append_raw(&meta_schema, metadata.avro())? +
               self.append_marker()?)
    }

    pub fn append<V>(&mut self, value: V) -> Result<usize, Error> where V: ToAvro {
        self.extend(once(value))
    }

    fn append_marker(&mut self) -> Result<usize, Error> {
        // using .writer.write directly to avoid mutable borrow of self
        // with ref borrowing of self.marker
        Ok(self.writer.write(&self.marker)?)
    }

    fn append_raw<V>(&mut self, schema: &Schema, value: V) -> Result<usize, Error> where V: EncodeAvro {
        match value.encode(schema) {
            Some(stream) => Ok(self.writer.write(stream.as_ref())?),
            None => Err(err_msg("value does not match given schema")),
        }
    }

    pub fn extend<I, V>(&mut self, values: I) -> Result<usize, Error>
        where V: ToAvro, I: Iterator<Item=V>
    {
        let mut num_values = 0;
        let mut stream = values
            .map(|value| value.avro().encode(self.schema))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| err_msg("value does not match given schema"))?
            .into_iter()
            .fold(Vec::new(), |mut acc, stream| {
                num_values += 1;
                acc.extend(stream); acc
            });

        stream = match self.codec {
            Codec::Null => stream,
            Codec::Deflate => {
                let mut encoder = Encoder::new(Vec::new());
                encoder.write(stream.as_ref())?;
                encoder.finish().into_result()?
            },
            #[cfg(feature = "snappy")] Codec::Snappy => {
                let mut writer = SnappyWriter::new(Vec::new());
                writer.write(stream.as_ref())?;
                writer.into_inner()?  // .into_inner() will also call .flush()
            },
        };

        if !self.has_header {
            self.header()?;
            self.has_header = true;
        }

        Ok(self.append_raw(&Schema::Long, num_values)? +
            self.append_raw(&Schema::Long, stream.len())? +
            self.writer.write(stream.as_ref())? +
            self.append_marker()?)
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}