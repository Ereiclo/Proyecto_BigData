import { useState } from "react";
import reactLogo from "./assets/react.svg";
import viteLogo from "/vite.svg";
// import './App.css'

function EntradaRecomendacion({ categoria, subcategoria, products, id }) {
  return (
    <div className="accordion-item">
      <h2 className="accordion-header">
        <button
          className="accordion-button collapsed"
          type="button"
          data-bs-toggle="collapse"
          data-bs-target="#collapseThree"
          aria-expanded="false"
          aria-controls="collapseThree"
        >
          Recomendacion #{id}
        </button>
      </h2>
      <div
        id="collapseThree"
        className="accordion-collapse collapse"
        data-bs-parent="#accordionExample"
      >
        <div className="accordion-body">
          <ul>
            <li>Categoria: {categoria}</li>
            <li>Subcategoria: {subcategoria}</li>
            <ul>
              {products.map((product, index) => (
                <li key={index}>
                  {product["descripcion"]}
                  <ul>
                  <li >{`Codigo: ${product['codigosap']}`}</li>
                  <li >{`Estimación gasto: ${Math.round(product['est_total_gastado']*100)/100}`}</li>
                  </ul>
                </li>
              ))}
            </ul>
            {/* <li>Productos: {products}</li> */}
          </ul>
        </div>
      </div>
    </div>
  );
}

function EntradaCompra({ categoria, subcategoria, total_gastado, id }) {
  return (
    <div className="accordion-item">
      <h2 className="accordion-header">
        <button
          className="accordion-button collapsed"
          type="button"
          data-bs-toggle="collapse"
          data-bs-target="#collapseThree"
          aria-expanded="false"
          aria-controls="collapseThree"
        >
          Compra #{id}
        </button>
      </h2>
      <div
        id="collapseThree"
        className="accordion-collapse collapse"
        data-bs-parent="#accordionExample"
      >
        <div className="accordion-body">
          <ul>
            <li>Categoria: {categoria}</li>
            <li>Subcategoria: {subcategoria}</li>
            <li>Total gastado: {total_gastado}</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

function App() {
  const [id, setId] = useState(0);
  const [compras, setCompras] = useState([]);
  const [recomendaciones, setRecomendaciones] = useState([]);

  const hacer_consulta = (event) => {
    event.preventDefault();
    fetch(`http://127.0.0.1:5000/${id}`)
      .then((response) => response.json())
      .then((data) => {
        setCompras(data["compras"]);
        setRecomendaciones(data["recomendaciones"]);
      })
      .catch((error) => {
        console.log(error);
      });
  };

  return (
    <>
      <h1 style={{marginLeft: "20px"}}>Ver Recomendaciones</h1>
      <form method="GET" className="row g-2" onSubmit={hacer_consulta}>
        <div className="col-auto">
          <input
            className="form-control"
            id="id"
            type="number"
            value={id}
            onChange={(e) => setId(e.target.value)}
          />
        </div>
        <div className="col-auto">
          <input
            type="submit"
            className="btn btn-primary mb-2"
            value="Buscar Cliente"
          />
        </div>
      </form>

      <div style={{ display: "flex" }}>
        <div style={{width: "50%"}} className="accordion" id="accordionExample">
          {compras.map((compra, index) => (
            <EntradaCompra
              key={index}
              categoria={compra["categoria"]}
              subcategoria={compra["subcategoria"]}
              total_gastado={compra["total_gastado"]}
              id={index + 1}
            />
          ))}
        </div>
        <div style={{width: "50%"}} className="accordion" id="accordionExample">
          {recomendaciones.map((recomendacion, index) => (
            <EntradaRecomendacion
              key={index}
              categoria={recomendacion["categoria"]}
              subcategoria={recomendacion["subcategoria"]}
              products={recomendacion["products"]}
              id={index + 1}
            />
          ))}
        </div>
      </div>
    </>
  );
}

export default App;
