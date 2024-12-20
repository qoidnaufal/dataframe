use proc_macro;

#[derive(Debug)]
enum ParseError {
    IdentNotFound
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug)]
enum Visibility {
    Pub,
    PubCrate,
    PubSuper,
    Private,
}

impl Visibility {
    fn to_str(&self) -> &str {
        match self {
            Visibility::Pub => "pub",
            Visibility::PubCrate => "pub(crate)",
            Visibility::PubSuper => "pub(super)",
            Visibility::Private => "",
        }
    }
}

#[derive(Debug)]
struct ParsedTokenStream {
    visibility: Visibility,
    name: proc_macro::Ident,
    generics: Option<Vec<proc_macro::TokenTree>>,
    data: Option<Vec<Vec<proc_macro::TokenTree>>>,
}

impl ParsedTokenStream {
    fn name(&self) -> proc_macro::Ident {
        self.name.clone()
    }

    fn generics(&self) -> Option<String> {
        if let Some(ref generics) = self.generics {
            Some(generics.iter().map(|tree| tree.to_string()).collect::<String>())
        } else { None }
    }

    fn lifetime(&self) -> Option<Vec<proc_macro::Ident>> {
        if let Some(ref generics) = self.generics {
            generics
                .iter()
                .map(|t| match t {
                    proc_macro::TokenTree::Ident(ident) => Some(ident.clone()),
                    _ => None
                })
                .collect::<Option<Vec<_>>>()
        } else { None }
    }

    fn fnames(&self) -> Option<Vec<String>> {
        if let Some(ref data) = self.data {
            Some(data.iter().map(|tree| tree[0].to_string()).collect())
        } else { None }
    }

    fn ftypes(&self) -> Option<Vec<String>> {
        if let Some(ref data) = self.data {
            Some(data.iter().map(|v| v[2..].iter().map(ToString::to_string).collect::<String>()).collect())
        } else { None }
    }

    // FIXME: better deserialization
    fn into_token_stream(&self) -> proc_macro::TokenStream {
        let visibility = self.visibility.to_str();
        let name = self.name();
        let _generics = self.generics();
        let _lifetime = self.lifetime();
        let fnames = self.fnames().unwrap();
        let ftypes = self.ftypes().unwrap();

        let token_stream: proc_macro::TokenStream = format!("
            use std::io::{{BufReader, Read}};
            use dataframe::{{DataFrame, Val}};

            impl {name} {{
                {visibility} fn read_csv(path: &str) -> Result<DataFrame, Error> {{
                    let file = std::fs::File::open(&path)?;
                    let mut buf = BufReader::new(file);

                    let mut s = String::new();
                    buf.read_to_string(&mut s)?;

                    Self::read_str(s)
                }}

                {visibility} fn read_str(input: String) -> Result<DataFrame, Error> {{
                    let mut raw_width = 0;
                    let mut raw_height = 0;
                    let raw = input
                        .lines()
                        .flat_map(|line| {{
                            raw_height += 1;
                            let l = line.split(\",\").map(ToString::to_string).collect::<Vec<_>>();
                            raw_width = l.len();
                            l
                        }})
                        .collect::<Vec<_>>();
                    let headers = raw[0..raw_width].to_vec();
                    let new_pos = {fnames:?}.iter().filter_map(|name| headers.iter().position(|header| header == name)).collect::<Vec<_>>();
                    raw_height -= 1;

                    let mut cursor = 0;
                    let mut adv = 0;
                    let slice = raw[raw_width..].to_vec();
                    let mut filtered_data = Vec::new();
                    while cursor < slice.len() {{
                        let pos = new_pos[cursor % new_pos.len()];
                        filtered_data.push(slice[pos + adv].to_string());
                        cursor += 1;
                        if cursor % new_pos.len() == 0 {{
                            adv += raw_width;
                        }}
                        if pos + adv > slice.len() {{ break }}
                    }}
                    
                    let data = filtered_data.iter().enumerate().map(|(i, d)| {{
                        let ftyp = {ftypes:?}[i % {ftypes:?}.len()];
                        let val = match ftyp {{
                            \"f64\" => {{Val::Float64(d.parse::<f64>().unwrap())}},
                            \"f32\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"usize\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"isize\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"u128\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"i128\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"u64\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"i64\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"u32\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"i32\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"u16\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"i16\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"u8\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"i8\" => {{Val::Usize(d.parse::<usize>().unwrap())}},
                            \"String\" => {{Val::String(d.to_string())}},
                            other => {{return Err(Error::InvalidDataType(other.to_string()))}}
                        }};
                        Ok::<Val, Error>(val)
                    }}).collect::<Result<Vec<Val>, Error>>()?;

                    let mut df = DataFrame::default();
                    df.set_headers({fnames:?}.iter().map(ToString::to_string).collect());
                    df.set_data(data);
                    df.set_size({fnames:?}.len(), raw_height);

                    Ok(df)
                }}
            }}
        ").parse().unwrap();

        token_stream
    }
}

struct Cursor {
    buffer: Vec<proc_macro::TokenTree>,
    offset: usize
}

impl Cursor {
    fn new(ts: proc_macro::TokenStream) -> Self {
        Self {
            buffer: ts.into_iter().collect(),
            offset: 0,
        }
    }

    fn parse(&mut self) -> Result<ParsedTokenStream, ParseError> {
        let mut visibility = Visibility::Private;
        let mut name: Option<proc_macro::Ident> = None;
        let mut generics: Option<Vec<proc_macro::TokenTree>> = None;
        let mut data: Option<Vec<Vec<proc_macro::TokenTree>>> = None;

        while self.offset < self.buffer.len() {
            match &self.buffer[self.offset] {
                proc_macro::TokenTree::Group(group) => {
                    let group_data = group.stream().into_iter().collect::<Vec<_>>();
                    // what's better? to include ',', or not?
                    let fields = group_data.split(|tree| {
                        match tree {
                            proc_macro::TokenTree::Punct(punct) => punct.as_char() == ',',
                            _ => false
                        }
                    }).filter(|trees| trees.len() > 0).map(|trees| trees.to_vec()).collect::<Vec<_>>();
                    data.replace(fields);
                },
                proc_macro::TokenTree::Ident(ident) => {
                    match ident.to_string().as_str() {
                        "pub" => {
                            if let proc_macro::TokenTree::Group(g) = &self.buffer[self.offset + 1] {
                                if g.delimiter() == proc_macro::Delimiter::Parenthesis {
                                    g.stream().into_iter().for_each(|tree| {
                                        if let proc_macro::TokenTree::Ident(v) = tree {
                                            if v.to_string() == "crate" {
                                                visibility = Visibility::PubCrate;
                                            } else if v.to_string() == "super" {
                                                visibility = Visibility::PubSuper;
                                            }
                                        }
                                    });
                                }
                            } else { visibility = Visibility::Pub }
                        }
                        "struct" | "enum" => {
                            self.offset += 1;
                            let proc_macro::TokenTree::Ident(n) = &self.buffer[self.offset] else { continue };
                            name.replace(n.clone());
                        }
                        _ => {}
                    }
                },
                proc_macro::TokenTree::Punct(punct) => {
                    match punct.as_char() {
                        '<' => {
                            let  closing = &self.buffer[self.offset..].iter().position(|tree| {
                                match tree {
                                    proc_macro::TokenTree::Punct(punct) => {
                                        punct.as_char() == '>'
                                    },
                                    _ => false
                                }
                            });
                            if let Some(closing) = closing {
                                generics.replace(
                                    self
                                        // what's better? to include '<' & '>', or not?
                                        .buffer[self.offset + 1..*closing + self.offset]
                                        .iter()
                                        .cloned()
                                        .collect::<Vec<_>>()
                                );
                            } else { continue }
                        },
                        _ => {}
                    }
                },
                proc_macro::TokenTree::Literal(_) => {},
            }
            self.offset += 1;
        }

        let Some(name) = name else { return Err(ParseError::IdentNotFound) };

        Ok(ParsedTokenStream {
            visibility,
            name,
            generics,
            data,
        })
    }
}

#[proc_macro_derive(Data)]
pub fn derive_data(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut cursor = Cursor::new(input);
    let parsed = cursor.parse().unwrap();

    parsed.into_token_stream()
}
