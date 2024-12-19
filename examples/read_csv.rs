use dataframe::{DataFrame, Error};

fn main() -> Result<(), Error> {
    let df = DataFrame::read_csv("examples/data.csv")?;
    println!("{:?}", df);

    Ok(())
}
