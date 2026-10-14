// Copyright (c) 2023-2024 Percona and/or its affiliates.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

#ifndef BINSRV_STORAGE_BACKEND_FACTORY_HPP
#define BINSRV_STORAGE_BACKEND_FACTORY_HPP

#include "binsrv/basic_storage_backend_fwd.hpp"
#include "binsrv/storage_config_fwd.hpp"

#ifdef BINSRV_STORAGE_BACKEND_TEST_HOOK
#include <functional>
#endif

namespace binsrv {

struct [[nodiscard]] storage_backend_factory {
  [[nodiscard]] static basic_storage_backend_ptr
  create(const storage_config &config);

#ifdef BINSRV_STORAGE_BACKEND_TEST_HOOK
  // Test-only seam, compiled in only with -DWITH_PROPERTY_TESTS=ON: when set,
  // every backend 'create()' makes is passed through 'decorator' and the
  // result is used instead, so that a test can wrap the real backend (for
  // instance to inject failures or simulate a crash at a chosen call). An
  // empty function (the default) restores the plain behaviour.
  using backend_decorator =
      std::function<basic_storage_backend_ptr(basic_storage_backend_ptr)>;
  static void set_test_decorator(backend_decorator decorator);
#endif
};

} // namespace binsrv

#endif // BINSRV_STORAGE_BACKEND_FACTORY_HPP
