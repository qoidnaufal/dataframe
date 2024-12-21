use dataframe::{Error, macros::DataFrame};

#[derive(DataFrame)]
struct MyData {
    nationality: String,
    name: String,
    xg: f64,
    goals: usize,
}

fn main() -> Result<(), Error> {
    let df = MyData::read_csv("examples/data.csv")?;
    println!("{:?}", df);

    Ok(())
}
