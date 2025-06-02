#pragma once

#include <vector>

namespace springtail {

template< typename T >
struct CircularBuffer {
	typedef T value_type;

	CircularBuffer(size_t max_size) {
        _b.resize(max_size);
	}

	bool full() const {
		return _cnt == _b.size();
	}
	bool empty() const {
		return _cnt == 0;
	}

	void put( T v ) {
		DCHECK(_cnt <= _b.size());
		DCHECK(_read_pnt < _b.size());
		// next index
		auto i = ( _read_pnt + _cnt ) % _b.size();
		_b[i] = v;
		++_cnt;
	}

	T next() {
		DCHECK(!empty());
		--_cnt;
		auto i = _read_pnt++;
		if (_read_pnt == _b.size())
			_read_pnt = 0;
		return _b[i];
	}

private:
	std::vector<value_type> _b;
	size_t _cnt = 0; // number of element in the buffer
	size_t _read_pnt = 0;

};

}
