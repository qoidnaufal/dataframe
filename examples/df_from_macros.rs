use dataframe::{Error, Data, Val};

#[derive(Data, Debug)]
struct MyData {
    name: String,
    nationality: String,
    xg: f64,
    goals: usize
}

fn main() -> Result<(), Error> {
    let df = MyData::read_csv("examples/data.csv")?;
    println!("{:?}", df);

    Ok(())
}
