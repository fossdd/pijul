#![recursion_limit = "256"]
extern crate proc_macro;
extern crate proc_macro2;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use proc_macro2::*;

use std::iter::FromIterator;

fn name_capital(name: &str) -> String {
    name.chars()
        .enumerate()
        .map(|(i, s)| {
            if i == 0 {
                s.to_uppercase().next().unwrap()
            } else {
                s
            }
        })
        .collect()
}

#[proc_macro]
pub fn table(input: proc_macro::TokenStream) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    assert!(input_iter.next().is_none());
    let name_capital = syn::Ident::new(&name_capital(&name), Span::call_site());
    proc_macro::TokenStream::from(quote! {
        type #name_capital;
    })
}

#[proc_macro]
pub fn sanakirja_table_get(input: proc_macro::TokenStream) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    let name_get = syn::Ident::new(&format!("get_{}", name), Span::call_site());
    let name = syn::Ident::new(&name, Span::call_site());
    let key = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let value = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let error = next(&mut input_iter);
    let error = if error.is_empty() {
        quote! { Error }
    } else {
        proc_macro2::TokenStream::from_iter(error.into_iter())
    };
    let txnerr = next(&mut input_iter);
    let txnerr = if txnerr.is_empty() {
        quote! { TxnErr }
    } else {
        proc_macro2::TokenStream::from_iter(txnerr.into_iter())
    };
    proc_macro::TokenStream::from(quote! {
        fn #name_get <'txn> (&'txn self, key: &#key, value: Option<&#value>) -> Result<Option<&'txn #value>, #txnerr<Self::#error>> {
            match ::sanakirja::btree::get(&self.txn, &self.#name, key, value) {
                Ok(Some((k, v))) if k == key => Ok(Some(v)),
                Ok(_) => Ok(None),
                Err(e) => {
                    error!("{:?}", e);
                    Err(#txnerr(SanakirjaError::PristineCorrupt))
                }
            }
        }
    })
}

#[proc_macro]
pub fn sanakirja_get(input: proc_macro::TokenStream) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    let name_capital = syn::Ident::new(&name_capital(&name), Span::call_site());
    let name_get = syn::Ident::new(&format!("get_{}", name), Span::call_site());
    let key = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let value = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let error = next(&mut input_iter);
    let error = if error.is_empty() {
        quote! { Error }
    } else {
        proc_macro2::TokenStream::from_iter(error.into_iter())
    };
    let txnerr = next(&mut input_iter);
    let txnerr = if txnerr.is_empty() {
        quote! { TxnErr }
    } else {
        proc_macro2::TokenStream::from_iter(txnerr.into_iter())
    };
    assert!(input_iter.next().is_none());
    proc_macro::TokenStream::from(quote! {
        fn #name_get<'txn>(&'txn self, db: &Self::#name_capital, key: &#key, value: Option<&#value>) -> Result<Option<&'txn #value>, #txnerr<Self::#error>> {
            match ::sanakirja::btree::get(&self.txn, db, key, value) {
                Ok(Some((k, v))) if k == key => Ok(Some(v)),
                Ok(_) => Ok(None),
                Err(e) => {
                    error!("{:?}", e);
                    Err(#txnerr(SanakirjaError::PristineCorrupt))
                }
            }
        }
    })
}

#[proc_macro]
pub fn table_get(input: proc_macro::TokenStream) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    let name_get = syn::Ident::new(&format!("get_{}", name), Span::call_site());
    let key = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let value = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let error = next(&mut input_iter);
    let error = if error.is_empty() {
        quote! { Error }
    } else {
        proc_macro2::TokenStream::from_iter(error.into_iter())
    };
    let txnerr = next(&mut input_iter);
    let txnerr = if txnerr.is_empty() {
        quote! { TxnErr }
    } else {
        proc_macro2::TokenStream::from_iter(txnerr.into_iter())
    };
    assert!(input_iter.next().is_none());
    proc_macro::TokenStream::from(quote! {
        fn #name_get<'txn>(&'txn self, key: &#key, value: Option<&#value>) -> Result<Option<&'txn #value>, #txnerr<Self::#error>>;
    })
}

#[proc_macro]
pub fn get(input: proc_macro::TokenStream) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    let name_capital = syn::Ident::new(&name_capital(&name), Span::call_site());
    let name_get = syn::Ident::new(&format!("get_{}", name), Span::call_site());
    let key = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let value = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let error = next(&mut input_iter);
    let error = if error.is_empty() {
        quote! { Error }
    } else {
        proc_macro2::TokenStream::from_iter(error.into_iter())
    };
    let txnerr = next(&mut input_iter);
    let txnerr = if txnerr.is_empty() {
        quote! { TxnErr }
    } else {
        proc_macro2::TokenStream::from_iter(txnerr.into_iter())
    };
    assert!(input_iter.next().is_none());
    proc_macro::TokenStream::from(quote! {
        fn #name_get<'txn>(&'txn self, db: &Self::#name_capital, key: &#key, value: Option<&#value>) -> Result<Option<&'txn #value>, #txnerr<Self::#error>>;
    })
}

fn next(input_iter: &mut proc_macro2::token_stream::IntoIter) -> Vec<TokenTree> {
    let mut result = Vec::new();
    let mut is_first = true;
    let mut level = 0;
    loop {
        match input_iter.next() {
            Some(TokenTree::Punct(p)) => {
                if p.as_char() == ',' {
                    if !is_first {
                        if level == 0 {
                            return result;
                        } else {
                            result.push(TokenTree::Punct(p))
                        }
                    }
                } else if p.as_char() == '<' {
                    level += 1;
                    result.push(TokenTree::Punct(p))
                } else if level > 0 && p.as_char() == '>' {
                    level -= 1;
                    result.push(TokenTree::Punct(p))
                } else {
                    result.push(TokenTree::Punct(p))
                }
            }
            Some(e) => result.push(e),
            None => return result,
        }
        is_first = false
    }
}

#[proc_macro]
pub fn cursor(input: proc_macro::TokenStream) -> TokenStream {
    cursor_(input, false, false, false)
}

#[proc_macro]
pub fn cursor_ref(input: proc_macro::TokenStream) -> TokenStream {
    cursor_(input, false, false, true)
}

#[proc_macro]
pub fn iter(input: proc_macro::TokenStream) -> TokenStream {
    cursor_(input, false, true, false)
}

#[proc_macro]
pub fn rev_cursor(input: proc_macro::TokenStream) -> TokenStream {
    cursor_(input, true, false, false)
}

fn cursor_(input: proc_macro::TokenStream, rev: bool, iter: bool, borrow: bool) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    let capital = name_capital(&name);
    let cursor_name = syn::Ident::new(&format!("{}Cursor", capital,), Span::call_site());
    let name_capital = syn::Ident::new(&name_capital(&name), Span::call_site());
    let name_iter = syn::Ident::new(&format!("iter_{}", name), Span::call_site());
    let name_next = syn::Ident::new(&format!("cursor_{}_next", name), Span::call_site());
    let name_prev = syn::Ident::new(&format!("cursor_{}_prev", name), Span::call_site());
    let name_cursor = syn::Ident::new(
        &format!("{}cursor_{}", if rev { "rev_" } else { "" }, name),
        Span::call_site(),
    );
    let name_cursor_ref = syn::Ident::new(
        &format!("{}cursor_{}_ref", if rev { "rev_" } else { "" }, name),
        Span::call_site(),
    );

    let key = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let value = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());

    let error = next(&mut input_iter);
    let error = if error.is_empty() {
        quote! { GraphError }
    } else {
        proc_macro2::TokenStream::from_iter(error.into_iter())
    };
    let txnerr = next(&mut input_iter);
    let txnerr = if txnerr.is_empty() {
        quote! { TxnErr }
    } else {
        proc_macro2::TokenStream::from_iter(txnerr.into_iter())
    };

    let cursor_type = if rev {
        quote! {
            Result<crate::pristine::RevCursor<Self, &'txn Self, Self::#cursor_name, #key, #value>, #txnerr<Self::#error>>
        }
    } else {
        quote! {
            Result<crate::pristine::Cursor<Self, &'txn Self, Self::#cursor_name, #key, #value>, #txnerr<Self::#error>>
        }
    };
    let def = if rev {
        quote! {}
    } else {
        quote! {
            type #cursor_name;
            fn #name_next <'txn> (
                &'txn self,
                cursor: &mut Self::#cursor_name,
            ) -> Result<Option<(&'txn #key, &'txn #value)>, #txnerr<Self::#error>>;
            fn #name_prev <'txn> (
                &'txn self,
                cursor: &mut Self::#cursor_name,
            ) -> Result<Option<(&'txn #key, &'txn #value)>, #txnerr<Self::#error>>;
        }
    };
    let borrow = if borrow {
        quote! {
        fn #name_cursor_ref<RT: std::ops::Deref<Target = Self>>(
            txn: RT,
            db: &Self::#name_capital,
            pos: Option<(&#key, Option<&#value>)>,
        ) -> Result<crate::pristine::Cursor<Self, RT, Self::#cursor_name, #key, #value>, #txnerr<Self::#error>>;
        }
    } else {
        quote! {}
    };
    let iter = if !iter {
        quote! {}
    } else {
        quote! {
            fn #name_iter <'txn> (
                &'txn self,
                k: &#key,
                v: Option<&#value>
            ) -> #cursor_type;
        }
    };
    assert!(input_iter.next().is_none());
    proc_macro::TokenStream::from(quote! {
        #def
        fn #name_cursor<'txn>(
            &'txn self,
            db: &Self::#name_capital,
            pos: Option<(&#key, Option<&#value>)>,
        ) -> #cursor_type;
        #borrow
        #iter
    })
}

#[proc_macro]
pub fn sanakirja_cursor(input: proc_macro::TokenStream) -> TokenStream {
    sanakirja_cursor_(input, false, false, false)
}

#[proc_macro]
pub fn sanakirja_cursor_ref(input: proc_macro::TokenStream) -> TokenStream {
    sanakirja_cursor_(input, false, false, true)
}

#[proc_macro]
pub fn sanakirja_iter(input: proc_macro::TokenStream) -> TokenStream {
    sanakirja_cursor_(input, false, true, false)
}

#[proc_macro]
pub fn sanakirja_rev_cursor(input: proc_macro::TokenStream) -> TokenStream {
    sanakirja_cursor_(input, true, false, false)
}

fn sanakirja_cursor_(
    input: proc_macro::TokenStream,
    rev: bool,
    iter: bool,
    borrow: bool,
) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    let cursor_name = syn::Ident::new(
        &format!("{}Cursor", name_capital(&name),),
        Span::call_site(),
    );

    let name_capital = syn::Ident::new(&name_capital(&name), Span::call_site());
    let name_next = syn::Ident::new(&format!("cursor_{}_next", name), Span::call_site());
    let name_prev = syn::Ident::new(&format!("cursor_{}_prev", name), Span::call_site());
    let name_cursor = syn::Ident::new(
        &format!("{}cursor_{}", if rev { "rev_" } else { "" }, name),
        Span::call_site(),
    );
    let name_cursor_ref = syn::Ident::new(
        &format!("{}cursor_{}_ref", if rev { "rev_" } else { "" }, name),
        Span::call_site(),
    );
    let name_iter = syn::Ident::new(
        &format!("{}iter_{}", if rev { "rev_" } else { "" }, name),
        Span::call_site(),
    );

    let name = syn::Ident::new(&name, Span::call_site());
    let key = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let value = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());

    let txnerr = next(&mut input_iter);
    let txnerr = if txnerr.is_empty() {
        quote! { TxnErr }
    } else {
        proc_macro2::TokenStream::from_iter(txnerr.into_iter())
    };

    let iter = if iter {
        quote! {
            fn #name_iter <'txn> (
                &'txn self,
                k: &#key,
                v: Option<&#value>
            ) -> Result<Cursor<Self, &'txn Self, Self::#cursor_name, #key, #value>, #txnerr<SanakirjaError>> {
                self.#name_cursor(&self.#name, Some((k, v)))
            }
        }
    } else {
        quote! {}
    };

    let borrow = if borrow {
        quote! {
            fn #name_cursor_ref <RT: std::ops::Deref<Target = Self>> (
                txn: RT,
                db: &Self::#name_capital,
                pos: Option<(&#key, Option<&#value>)>,
            ) -> Result<Cursor<Self, RT, Self::#cursor_name, #key, #value>, #txnerr<SanakirjaError>> {
                let mut cursor = ::sanakirja::btree::cursor::Cursor::new(&txn.txn, &db)?;
                if let Some((k, v)) = pos {
                    cursor.set(&txn.txn, k, v)?;
                }
                Ok(Cursor {
                    cursor,
                    txn,
                    k: std::marker::PhantomData,
                    v: std::marker::PhantomData,
                    t: std::marker::PhantomData,
                })
            }
        }
    } else {
        quote! {}
    };

    proc_macro::TokenStream::from(if rev {
        quote! {
            fn #name_cursor<'txn>(
                &'txn self,
                db: &Self::#name_capital,
                pos: Option<(&#key, Option<&#value>)>,
            ) -> Result<super::RevCursor<Self, &'txn Self, Self::#cursor_name, #key, #value>, #txnerr<SanakirjaError>> {
                let mut cursor = ::sanakirja::btree::cursor::Cursor::new(&self.txn, &db)?;
                if let Some((k, v)) = pos {
                    cursor.set(&self.txn, k, v)?;
                } else {
                    cursor.set_last(&self.txn)?;
                }
                Ok(super::RevCursor {
                    cursor,
                    txn: self,
                    k: std::marker::PhantomData,
                    v: std::marker::PhantomData,
                    t: std::marker::PhantomData,
                })
            }
        }
    } else {
        quote! {
            fn #name_cursor<'txn>(
                &'txn self,
                db: &Self::#name_capital,
                pos: Option<(&#key, Option<&#value>)>,
            ) -> Result<Cursor<Self, &'txn Self, Self::#cursor_name, #key, #value>, #txnerr<SanakirjaError>> {
                let mut cursor = ::sanakirja::btree::cursor::Cursor::new(&self.txn, &db)?;
                if let Some((k, v)) = pos {
                    cursor.set(&self.txn, k, v)?;
                }
                Ok(Cursor {
                    cursor,
                    txn: self,
                    k: std::marker::PhantomData,
                    v: std::marker::PhantomData,
                    t: std::marker::PhantomData,
                })
            }
            #borrow
            fn #name_next <'txn> (
                &'txn self,
                cursor: &mut Self::#cursor_name,
            ) -> Result<Option<(&'txn #key, &'txn #value)>, #txnerr<SanakirjaError>> {
                let x = if let Ok(x) = cursor.next(&self.txn) {
                    x
                } else {
                    return Err(#txnerr(SanakirjaError::PristineCorrupt))
                };
                Ok(x)
            }
            fn #name_prev <'txn> (
                &'txn self,
                cursor: &mut Self::#cursor_name,
            ) -> Result<Option<(&'txn #key, &'txn #value)>, #txnerr<SanakirjaError>> {
                let x = if let Ok(x) = cursor.prev(&self.txn) {
                    x
                } else {
                    return Err(#txnerr(SanakirjaError::PristineCorrupt))
                };
                Ok(x)
            }
            #iter
        }
    })
}

#[proc_macro]
pub fn initialized_cursor(input: proc_macro::TokenStream) -> TokenStream {
    initialized_cursor_(input, false)
}

#[proc_macro]
pub fn initialized_rev_cursor(input: proc_macro::TokenStream) -> TokenStream {
    initialized_cursor_(input, true)
}

fn initialized_cursor_(input: proc_macro::TokenStream, rev: bool) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    let cursor_name = syn::Ident::new(
        &format!("{}Cursor", name_capital(&name),),
        Span::call_site(),
    );
    let name_next = syn::Ident::new(&format!("cursor_{}_next", name), Span::call_site());
    let name_prev = syn::Ident::new(&format!("cursor_{}_prev", name), Span::call_site());
    let key = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let value = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());

    let txnt = next(&mut input_iter);
    let txnt: proc_macro2::TokenStream = if txnt.is_empty() {
        proc_macro2::TokenStream::from(quote! { TxnT })
    } else {
        proc_macro2::TokenStream::from_iter(txnt.into_iter())
    };

    let error = next(&mut input_iter);
    let error: proc_macro2::TokenStream = if error.is_empty() {
        proc_macro2::TokenStream::from(quote! { GraphError })
    } else {
        proc_macro2::TokenStream::from_iter(error.into_iter())
    };

    let txnerr = next(&mut input_iter);
    let txnerr = if txnerr.is_empty() {
        quote! { TxnErr }
    } else {
        proc_macro2::TokenStream::from_iter(txnerr.into_iter())
    };

    assert!(input_iter.next().is_none());
    if rev {
        proc_macro::TokenStream::from(quote! {
            impl<'a, T: #txnt> Iterator for crate::pristine::RevCursor<T, &'a T, T::#cursor_name, #key, #value>
            {
                type Item = Result<(&'a #key, &'a #value), #txnerr<T::#error>>;
                fn next(&mut self) -> Option<Self::Item> {
                    match self.txn.#name_prev(&mut self.cursor) {
                        Ok(Some(x)) => Some(Ok(x)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
            }
        })
    } else {
        proc_macro::TokenStream::from(quote! {
            impl<'a, T: #txnt>
                crate::pristine::Cursor<T, &'a T, T::#cursor_name, #key, #value>
            {
                pub fn prev(&mut self) -> Option<Result<(&'a #key, &'a #value), #txnerr<T::#error>>> {
                    match self.txn.#name_prev(&mut self.cursor) {
                        Ok(Some(x)) => Some(Ok(x)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
            }
            impl<'a, T: #txnt> Iterator for crate::pristine::Cursor<T, &'a T, T::#cursor_name, #key, #value>
            {
                type Item = Result<(&'a #key, &'a #value), #txnerr<T::#error>>;
                fn next(&mut self) -> Option<Self::Item> {
                    match self.txn.#name_next(&mut self.cursor) {
                        Ok(Some(x)) => Some(Ok(x)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
            }
        })
    }
}

#[proc_macro]
pub fn put_del(input: proc_macro::TokenStream) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    let put = syn::Ident::new(&format!("put_{}", name), Span::call_site());
    let del = syn::Ident::new(&format!("del_{}", name), Span::call_site());

    let key = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let value = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());

    let error = next(&mut input_iter);
    let error = if error.is_empty() {
        quote! { Error }
    } else {
        proc_macro2::TokenStream::from_iter(error.into_iter())
    };

    let txnerr = next(&mut input_iter);
    let txnerr = if txnerr.is_empty() {
        quote! { TxnErr }
    } else {
        proc_macro2::TokenStream::from_iter(txnerr.into_iter())
    };
    assert!(input_iter.next().is_none());
    proc_macro::TokenStream::from(quote! {
        fn #put(
            &mut self,
            k: &#key,
            e: &#value,
        ) -> Result<bool, #txnerr<Self::#error>>;
        fn #del(
            &mut self,
            k: &#key,
            e: Option<&#value>,
        ) -> Result<bool, #txnerr<Self::#error>>;
    })
}

#[proc_macro]
pub fn sanakirja_put_del(input: proc_macro::TokenStream) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    let mut input_iter = input.into_iter();
    let name = match input_iter.next() {
        Some(TokenTree::Ident(id)) => id.to_string(),
        _ => panic!("txn_table: first argument not an identifier"),
    };
    let put = syn::Ident::new(&format!("put_{}", name), Span::call_site());
    let del = syn::Ident::new(&format!("del_{}", name), Span::call_site());
    let name = syn::Ident::new(&name, Span::call_site());

    let key = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());
    let value = proc_macro2::TokenStream::from_iter(next(&mut input_iter).into_iter());

    let error = next(&mut input_iter);
    let error = if error.is_empty() {
        quote! { Error }
    } else {
        proc_macro2::TokenStream::from_iter(error.into_iter())
    };

    let txnerr = next(&mut input_iter);
    let txnerr = if txnerr.is_empty() {
        quote! { TxnErr }
    } else {
        proc_macro2::TokenStream::from_iter(txnerr.into_iter())
    };
    assert!(input_iter.next().is_none());
    proc_macro::TokenStream::from(quote! {
        fn #put(
            &mut self,
            k: &#key,
            v: &#value,
        ) -> Result<bool, #txnerr<Self::#error>> {
            Ok(::sanakirja::btree::put(&mut self.txn, &mut self.#name, k, v).map_err(#txnerr)?)
        }
        fn #del(
            &mut self,
            k: &#key,
            v: Option<&#value>,
        ) -> Result<bool, #txnerr<Self::#error>> {
            Ok(::sanakirja::btree::del(&mut self.txn, &mut self.#name, k, v).map_err(#txnerr)?)
        }
    })
}
