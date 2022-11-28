//! Mocks for `WorkflowModule`s.

use mimicry::{Answers, CallReal, Mock, MockGuard, Mut};
use wasmtime::StoreContextMut;

use std::{
    collections::{HashMap, HashSet},
    task::Poll,
};

use super::*;
use crate::module::WorkflowModule;
use tardigrade::{TaskId, WakerId};

