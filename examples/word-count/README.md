# Word Count Example

This example demonstrates how to use Fluxus to count the number of words in input text, filter common stop words, and sort the results by word frequency.

## Features

- Count the number of words in input text.
- Support filtering common stop words.
- Output results sorted by word frequency.

## Running the Example

```bash
cargo run
```

## Implementation Details

- Use a streaming processing framework to process text data.
- Tokenize the input text.
- Count the occurrence of each word.
- Filter stop words and sort the output.

## Output Example

```
Word count results:
The: 10
And: 8
...
```

## Dependencies

- fluxus - core
- fluxus - runtime
- fluxus - api
- tokio
- anyhow