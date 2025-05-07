use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::hash::Hash;

use fluxus_transformers::operator::{WindowAllOperator, WindowAnyOperator};
use fluxus_utils::window::WindowConfig;

use crate::operators::{
    SortOrder, WindowAggregator, WindowSkipper, WindowSorter, WindowTimestampSorter,
};
use crate::stream::datastream::DataStream;

/// Represents a windowed stream for aggregation operations
pub struct WindowedStream<T> {
    pub(crate) stream: DataStream<T>,
    pub(crate) window_config: WindowConfig,
}

impl<T> WindowedStream<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Aggregate values in the window
    pub fn aggregate<A, F>(self, init: A, f: F) -> DataStream<A>
    where
        A: Clone + Send + Sync + 'static,
        F: Fn(A, T) -> A + Send + Sync + 'static,
    {
        let aggregator = WindowAggregator::new(self.window_config, init, f);
        self.stream.transform(aggregator)
    }

    pub fn any<F>(self, f: F) -> DataStream<bool>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let anyer = WindowAnyOperator::new(f, self.window_config);
        self.stream.transform(anyer)
    }

    pub fn all<F>(self, f: F) -> DataStream<bool>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let aller = WindowAllOperator::new(f, self.window_config);
        self.stream.transform(aller)
    }

    /// Limit the number of values in the window
    pub fn limit(self, n: usize) -> DataStream<Vec<T>> {
        let limiter = WindowAggregator::new(self.window_config, vec![], move |mut acc, value| {
            if acc.len() < n {
                acc.push(value);
            }
            acc
        });
        self.stream.transform(limiter)
    }

    /// Retain last n values in the window
    pub fn tail(self, n: usize) -> DataStream<Vec<T>> {
        let init = VecDeque::with_capacity(n);
        let limiter = WindowAggregator::new(self.window_config, init, move |mut acc, value| {
            acc.push_back(value);
            if acc.len() > n {
                acc.pop_front();
            }
            acc
        });
        self.stream
            .transform(limiter)
            .map(|d| d.into_iter().collect())
    }

    /// Sort values in the window
    pub fn sort_by<F>(self, f: F) -> DataStream<Vec<T>>
    where
        F: FnMut(&T, &T) -> Ordering + Send + Sync + 'static,
    {
        let sorter = WindowSorter::new(self.window_config, f);
        self.stream.transform(sorter)
    }

    /// Sort values in the window by timestamp
    pub fn sort_by_ts(self, order: SortOrder) -> DataStream<Vec<T>> {
        let sorter = WindowTimestampSorter::new(self.window_config, order);
        self.stream.transform(sorter)
    }

    /// Sort values in the window by timestamp in ascending order
    pub fn sort_by_ts_asc(self) -> DataStream<Vec<T>> {
        let sorter = WindowTimestampSorter::new(self.window_config, SortOrder::Asc);
        self.stream.transform(sorter)
    }

    /// Sort values in the window by timestamp in descending order
    pub fn sort_by_ts_desc(self) -> DataStream<Vec<T>> {
        let sorter = WindowTimestampSorter::new(self.window_config, SortOrder::Desc);
        self.stream.transform(sorter)
    }

    /// Skip
    pub fn skip(self, n: usize) -> DataStream<Vec<T>> {
        let skipper = WindowSkipper::new(self.window_config, n);
        self.stream.transform(skipper)
    }
}

impl<T> WindowedStream<T>
where
    T: Ord + Clone + Send + Sync + 'static,
{
    /// Sort values in specified order
    pub fn sort(self, ord: SortOrder) -> DataStream<Vec<T>> {
        self.sort_by(move |v1, v2| match ord {
            SortOrder::Asc => v1.cmp(v2),
            SortOrder::Desc => v2.cmp(v1),
        })
    }

    /// Get the top k values in the window, the values are sorted in descending order
    pub fn top_k(self, k: usize) -> DataStream<Vec<T>> {
        let init = BinaryHeap::<Reverse<T>>::new();
        let res = self.aggregate(init, move |mut heap, v| {
            heap.push(Reverse(v));
            if heap.len() > k {
                heap.pop();
            }
            heap
        });
        res.map(|heap| {
            heap.into_sorted_vec()
                .into_iter()
                .map(|Reverse(v)| v)
                .collect()
        })
    }
}

impl<T> WindowedStream<T>
where
    T: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Distinct values
    pub fn distinct(self) -> DataStream<HashSet<T>> {
        self.aggregate(HashSet::new(), |mut set, value| {
            set.insert(value);
            set
        })
    }
}

impl<T> WindowedStream<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Distinct values by key. When the same key is encountered, the first occurrence of the value is retained
    pub fn distinct_by_key<F, K>(self, f: F) -> DataStream<Vec<T>>
    where
        F: Fn(&T) -> K + Sync + Send + 'static,
        K: Eq + Hash + Clone + Sync + Send + 'static,
    {
        let keys = HashSet::new();
        let data = vec![];
        self.aggregate((keys, data), move |(mut keys, mut data), value| {
            let k = f(&value);
            if !keys.contains(&k) {
                keys.insert(k);
                data.push(value);
            }
            (keys, data)
        })
        .map(|(_, data)| data)
    }

    /// Get top k values by key. The values are sorted by key in descending order
    pub fn top_k_by_key<F, K>(self, n: usize, f: F) -> DataStream<Vec<T>>
    where
        F: Fn(&T) -> K + Sync + Send + 'static,
        K: Ord + Eq + Hash + Clone + Sync + Send + 'static,
    {
        // Store the top k keys
        let keys = BinaryHeap::<Reverse<K>>::new();
        // Store the values by key
        let kvs: HashMap<K, Vec<T>> = HashMap::new();
        self.aggregate((keys, kvs), move |(mut keys, mut kvs), value| {
            let k = f(&value);

            keys.push(Reverse(k.clone()));
            kvs.entry(k).or_default().push(value);

            if keys.len() > n {
                if let Some(Reverse(min_k)) = keys.pop() {
                    kvs.get_mut(&min_k).map(|v| v.pop());
                }
            }
            (keys, kvs)
        })
        .map(|(top_keys, mut kvs)| {
            top_keys
                .into_sorted_vec()
                .into_iter()
                .fold(vec![], move |mut acc, Reverse(k)| {
                    let values = kvs.remove(&k).unwrap_or_default();
                    acc.extend(values);
                    acc
                })
        })
    }
}
