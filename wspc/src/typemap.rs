use std::any;

type Inner = dashmap::DashMap<any::TypeId, Box<dyn any::Any + Sync + Send>>;

#[derive(Debug, Default)]
pub(crate) struct TypeMap {
	inner: Inner,
}

impl TypeMap {
	#[inline(always)]
	pub(crate) fn new() -> Self {
		Self::default()
	}
	pub(crate) fn set<T: 'static + Sync + Send>(&self, value: T) -> Option<T> {
		let type_id = any::TypeId::of::<T>();
		let value = Box::new(value);

		self.inner.insert(type_id, value).and_then(|value| value.downcast::<T>().ok()).map(|value| *value)
	}
	pub(crate) fn get<T: 'static + Clone>(&self) -> Option<T> {
		let type_id = any::TypeId::of::<T>();

		self.inner.get(&type_id).and_then(|value| value.downcast_ref::<T>().cloned())
	}
}
