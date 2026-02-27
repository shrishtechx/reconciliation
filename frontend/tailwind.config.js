/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        navy: {
          50: '#f0f3f9',
          100: '#d9e0f0',
          200: '#b3c1e0',
          300: '#8da2d1',
          400: '#6783c1',
          500: '#4a6fa5',
          600: '#3a5a8a',
          700: '#2a4570',
          800: '#1B2A4A',
          900: '#111d35',
        },
      },
    },
  },
  plugins: [],
}
