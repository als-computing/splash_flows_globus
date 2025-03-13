/**
 * App.jsx
 *
 * The main application component. Renders a top header bar and an iframe that
 * takes up the remaining vertical space in the window.
 *
 * Usage:
 *   <App />
 *
 * @return {JSX.Element} A full-page layout with a header and auto-resizing iframe.
 */

import React from 'react';
import { Header } from './Header';
import ItkVtkViewerComponent from './ItkVtkViewerComponent';

import './App.css';

function App() {

  // TODO: Load the file URL from Tiled

  // Tiled file URL
  // to start tiled:
  // TILED_ALLOW_ORIGINS="http://localhost:3000 http://localhost:5174 http://localhost:8082" tiled serve directory "../../../data/tomo/scratch/" --public --verbose
  const file_url = 'http://localhost:8000/zarr/v2/rec20230606_152011_jong-seto_fungal-mycelia_flat-AQ_fungi2_fast';
  const fileName = file_url.split('/').pop();
  
  // itk-vtk-viewer iframe source
  // to start: run "itk-vtk-viewer --port 8082" in the terminal (base)
  const iframeSrc =
    'http://localhost:8082/?fileToLoad='+file_url;

  return (
    <div id="app">
      <Header 
        logoUrl='/images/als_logo_wheel.png'
        title="Tomography Visualizer powered by itk-vtk-viewer"
        fileName={fileName}>
      </Header>
      <ItkVtkViewerComponent
        src={iframeSrc}
        height="100%"
        flex="1"
      />
    </div>
  );
}

export default App;
